# -*- coding: utf-8 -*-
"""
Оркестратор обработки входных файлов.

Пайплайн (укороченно):
1) Захватываем advisory lock в Postgres (единственный запуск).
2) (Опционально) чистим "Итоговые отчеты" от старых артефактов.
3) Читаем из БД ops.file_registry записи со статусами NEW/PROCESSING/ERROR и делаем CSV (read-only).
4) Для каждой строки:
   4.1) Находим клиентский скрипт. Если нет — ставим PROCESSING (reason=NO_SCRIPT_FOUND), идем дальше.
   4.2) Ставим PROCESSING (reason=NULL), запускаем клиентский скрипт (передаем TASK_ID в env).
   4.3) При успехе ищем файлы для данного id, переносим в "Данные на загрузку".
        - если перенесли >=1 — ставим CREATED, error_reason=NULL
        - иначе — ставим ERROR (reason=NO_OUTPUT_FILE)
   4.4) При неуспехе — ставим ERROR (reason по коду/исключению).
5) Освобождаем advisory lock.
"""

import os
import re
import csv
import sys
import time
import shutil
import hashlib
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import psycopg2

# --- Безопасный вывод: никогда не падаем на символах из-за локали ---
try:
    enc = os.environ.get("PYTHONIOENCODING") or "utf-8"
    sys.stdout.reconfigure(encoding=enc, errors="replace")
    sys.stderr.reconfigure(encoding=enc, errors="replace")
except Exception:
    pass

# === НАСТРОЙКИ ПУТЕЙ ===
REESTR_DIR = r"C:\Users\user\Desktop\Python_scripts\automated_processing\Reestr"
OUTPUT_NAME = "new_files_registry.csv"
TMP_NAME = OUTPUT_NAME + ".tmp"

SCRIPTS_BASE = r"C:\Users\user\Desktop\Python_scripts\automated_processing\Scripts"
FINAL_DIR = r"C:\Users\user\Desktop\Итоговые отчеты"       # сюда пишут клиентские скрипты
LOAD_DIR  = r"C:\Users\user\Desktop\Данные на загрузку"    # сюда переносим валидные файлы

# === ПОДКЛЮЧЕНИЕ К БД ===
DB = dict(
    host="localhost",
    port=5432,
    database="etl_demo",
    user="postgres",
    password="X9g4e153tyuF2",
)

# === СТАТУСЫ ===
STAT_NEW        = "NEW"
STAT_PROC       = "PROCESSING"
STAT_CREATED    = "CREATED"
STAT_ERROR      = "ERROR"
ALLOWED_STATUSES = {STAT_NEW, STAT_PROC, STAT_CREATED, STAT_ERROR}

# === ПАРАМЕТРЫ ИСПОЛНЕНИЯ ===
PYTHON_EXE = sys.executable
SCRIPT_TIMEOUT_SEC = 1800          # таймаут клиентского скрипта (30 мин)
ADVISORY_KEY = 84215045            # любой фиксированный int64
CLEANUP_STRATEGY = "age"           # "age" | "all" — чистить старые файлы или удалять все
CLEANUP_OLDER_THAN_MIN = 60        # для "age": удалять артефакты старше N минут
MOVE_MAX_RETRIES = 5               # попытки переноса при временных ошибках
MOVE_RETRY_SLEEP = 4               # пауза между попытками, сек

# === СТОЛБЦЫ CSV (для просмотра) ===
COLUMNS = [
    "id",
    "file_path",
    "status",
    "data_provider",
    "report_year",
    "report_month",
    "client_name",
    "report_type",
    "uploaded_at",
    "created_at",
    "script",
]

# ========== УТИЛИТЫ ==========

def ensure_dir(path: str) -> None:
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

def get_csv_path() -> str:
    return os.path.join(REESTR_DIR, OUTPUT_NAME)

def get_tmp_path() -> str:
    return os.path.join(REESTR_DIR, TMP_NAME)

def get_script_path(data_provider: str, client_name: str) -> str:
    """Определяем путь к клиентскому скрипту."""
    if data_provider == "Дистрибьютор":
        base_folder = os.path.join(SCRIPTS_BASE, "Distibutors")  # оставлено как есть
    elif data_provider == "Сеть":
        base_folder = os.path.join(SCRIPTS_BASE, "Nets")
    else:
        return "NO_SCRIPT_FOUND"

    client_folder = os.path.join(base_folder, client_name)
    script_file = os.path.join(client_folder, f"{client_name}_processing.py")
    return script_file if os.path.isfile(script_file) else "NO_SCRIPT_FOUND"

def sha256sum(p: Path, chunk: int = 2**20) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def safe_remove(p: Path) -> None:
    try:
        p.unlink(missing_ok=True)
    except Exception:
        pass

# ========== БД ВСПОМОГАТЕЛЬНЫЕ ==========

def db_connect():
    return psycopg2.connect(**DB)

def db_try_advisory_lock(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s);", (ADVISORY_KEY,))
        locked, = cur.fetchone()
        return bool(locked)

def db_advisory_unlock(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s);", (ADVISORY_KEY,))

def db_update_status(conn, _id: int, status: str, error_reason: str | None = None) -> None:
    if status not in ALLOWED_STATUSES:
        raise ValueError(f"Недопустимый статус: {status}")
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE ops.file_registry SET status = %s, error_reason = %s WHERE id = %s;",
            (status, error_reason, _id),
        )
    conn.commit()

def fetch_registry_rows(conn):
    """Берем из БД NEW/PROCESSING/ERROR для обработки."""
    sql = """
        SELECT
            id,
            file_path,
            status,
            data_provider,
            report_year,
            report_month,
            client_name,
            report_type,
            uploaded_at,
            created_at
        FROM ops.file_registry
        WHERE status IN ('NEW','PROCESSING','ERROR')
        ORDER BY uploaded_at;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    result = []
    for row in rows:
        row = list(row)
        data_provider = row[3]
        client_name = row[6]
        script_path = get_script_path(data_provider, client_name)
        row.append(script_path)
        result.append(row)
    return result

def write_csv_atomic(rows) -> str:
    ensure_dir(REESTR_DIR)
    out_path = get_csv_path()
    tmp_path = get_tmp_path()
    with open(tmp_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(COLUMNS)
        for r in rows:
            w.writerow(r)
    os.replace(tmp_path, out_path)
    return out_path

def write_empty_marker() -> str:
    ensure_dir(REESTR_DIR)
    out_path = get_csv_path()
    tmp_path = get_tmp_path()
    with open(tmp_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(COLUMNS)
        w.writerow(["", "", "NO_TASKS", "", "", "", "", "", datetime.now().isoformat(sep=" "), "", ""])
    os.replace(tmp_path, out_path)
    return out_path

# ========== РАБОТА С ФАЙЛАМИ ==========

def cleanup_final_dir(strategy: str = CLEANUP_STRATEGY, older_than_min: int = CLEANUP_OLDER_THAN_MIN) -> None:
    """Осторожная очистка итоговой папки."""
    Path(FINAL_DIR).mkdir(parents=True, exist_ok=True)
    if strategy == "all":
        for p in Path(FINAL_DIR).iterdir():
            if p.is_file():
                safe_remove(p)
        return

    cutoff = time.time() - older_than_min * 60
    for p in Path(FINAL_DIR).iterdir():
        if p.is_file():
            try:
                if p.stat().st_mtime < cutoff:
                    safe_remove(p)
            except Exception:
                pass

def files_for_id(dir_path: str, _id: int, since_ts: float | None) -> list[Path]:
    """Ищем файлы для конкретного id. Если since_ts задан — фильтруем по времени."""
    root = Path(dir_path)
    if not root.is_dir():
        return []
    pat = re.compile(rf'[_\-]id{_id}(?:[_\.\-]|$)', re.IGNORECASE)
    out: list[Path] = []
    for name in os.listdir(dir_path):
        p = root / name
        if not p.is_file():
            continue
        try:
            if since_ts is None or p.stat().st_mtime >= since_ts:
                if pat.search(name):
                    out.append(p)
        except OSError:
            continue
    return out

def move_with_retries(src: Path, load_dir: Path, max_retries: int = MOVE_MAX_RETRIES, sleep_sec: int = MOVE_RETRY_SLEEP) -> tuple[bool, str, Path | None]:
    """Перенос файла в LOAD_DIR с ретраями и защитой от коллизий."""
    load_dir.mkdir(parents=True, exist_ok=True)
    dst = load_dir / src.name

    if dst.exists():
        try:
            if dst.stat().st_size == src.stat().st_size and sha256sum(dst) == sha256sum(src):
                return True, "ALREADY_PRESENT", dst
        except Exception:
            pass
        root, ext = os.path.splitext(src.name)
        dst = load_dir / f"{root}_{int(time.time())}{ext}"

    for _ in range(max_retries):
        try:
            shutil.move(str(src), str(dst))
            return True, "OK", dst
        except PermissionError:
            time.sleep(sleep_sec)
        except OSError as e:
            winerr = getattr(e, "winerror", None)
            if winerr in (5, 32, 33):  # access denied / sharing violation
                time.sleep(sleep_sec)
                continue
            if winerr == 206:          # path too long
                return False, "PATH_TOO_LONG", None
            if e.errno in (28,):       # no space
                return False, "NO_SPACE", None
            return False, f"OSERROR:{e.errno or winerr}", None
    return False, "LOCKED", None

# ========== ОСНОВНАЯ ЛОГИКА ==========

def run_pipeline():
    run_start_ts = time.time()
    ensure_dir(REESTR_DIR)
    ensure_dir(FINAL_DIR)
    ensure_dir(LOAD_DIR)

    with db_connect() as conn:
        if not db_try_advisory_lock(conn):
            print("[WARN] Оркестратор уже запущен - выходим.")
            return
        try:
            print(f"[STEP] Очистка '{FINAL_DIR}' (strategy={CLEANUP_STRATEGY})...")
            cleanup_final_dir()

            rows = fetch_registry_rows(conn)
            if rows:
                out_csv = write_csv_atomic(rows)
                print(f"[STEP] Реестр сформирован: {out_csv} | записей: {len(rows)}")
            else:
                out_csv = write_empty_marker()
                print(f"[STEP] Задач нет. Обновлен пустой реестр: {out_csv}")
                return

            print("\n[STEP] Запуск клиентских скриптов по реестру...")
            any_launched = False

            for r in rows:
                (_id, file_path, status, data_provider, report_year, report_month,
                 client_name, report_type, uploaded_at, created_at, script) = r

                if not script or script == "NO_SCRIPT_FOUND" or not os.path.isfile(script):
                    print(f" - id={_id} скрипт не найден -> PROCESSING(reason=NO_SCRIPT_FOUND)")
                    db_update_status(conn, _id, STAT_PROC, "NO_SCRIPT_FOUND")
                    continue

                # ставим PROCESSING и запускаем
                db_update_status(conn, _id, STAT_PROC, None)
                any_launched = True
                print(f" - id={_id} запускаю: {script}")

                # Передаем TASK_ID и метаданные в окружение
                env = os.environ.copy()
                env.update({
                    "TASK_ID": str(_id),
                    "TASK_CLIENT": str(client_name or ""),
                    "TASK_FILE": str(file_path or ""),
                    "TASK_REPORT_TYPE": str(report_type or "")
                })

                try:
                    proc = subprocess.run(
                        [PYTHON_EXE, script],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        timeout=SCRIPT_TIMEOUT_SEC,
                        env=env,
                    )
                except subprocess.TimeoutExpired:
                    print(f"   TIMEOUT ({script}) > {SCRIPT_TIMEOUT_SEC}s")
                    db_update_status(conn, _id, STAT_ERROR, "TIMEOUT")
                    continue
                except Exception as e:
                    print(f"   ERROR запуск {script}: {e}")
                    db_update_status(conn, _id, STAT_ERROR, f"LAUNCH_ERROR:{e}")
                    continue

                # печатаем хвосты логов даже при returncode==0 (если есть)
                if proc.stdout:
                    print("   STDOUT(last 1000):\n", proc.stdout[-1000:])
                if proc.stderr:
                    print("   STDERR(last 1000):\n", proc.stderr[-1000:])

                if proc.returncode != 0:
                    print(f"   FAIL code={proc.returncode}")
                    db_update_status(conn, _id, STAT_ERROR, f"RETURN_CODE_{proc.returncode}")
                    continue

                # ищем и переносим файлы для id — сперва по времени запуска, затем фолбэк "без времени"
                out_files = files_for_id(FINAL_DIR, _id, run_start_ts)
                if not out_files:
                    out_files = files_for_id(FINAL_DIR, _id, None)

                if not out_files:
                    print(f"   WARN: нет файлов для id={_id} в '{FINAL_DIR}'")
                    db_update_status(conn, _id, STAT_ERROR, "NO_OUTPUT_FILE")
                    continue

                moved = 0
                last_reason = "OK"
                for src in out_files:
                    ok, reason, dst = move_with_retries(Path(src), Path(LOAD_DIR))
                    last_reason = reason
                    if ok:
                        moved += 1
                    else:
                        print(f"   WARN: не смог перенести '{src.name}' -> {reason}")

                if moved > 0:
                    print(f"   OK: перенесено файлов={moved}, статус -> CREATED")
                    db_update_status(conn, _id, STAT_CREATED, None)
                else:
                    print(f"   ERROR: ни один файл не перенесен (последняя причина: {last_reason})")
                    db_update_status(conn, _id, STAT_ERROR, last_reason)

            if not any_launched:
                print("   Нет скриптов для запуска (все NO_SCRIPT_FOUND).")

        finally:
            db_advisory_unlock(conn)
            print("[STEP] Advisory lock снят. Завершено.")

# ========== ENTRYPOINT ==========

if __name__ == "__main__":
    run_pipeline()
