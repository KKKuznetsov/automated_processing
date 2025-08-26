# automated_processing

Данная программа предназначена для автоматической обработки входных данных и приведения их под единый шаблон (схему целевых колонок).
Предназначена только для демонстрации кода.

# Блок-схема программы.
<img width="1801" height="911" alt="image" src="https://github.com/user-attachments/assets/9f56afef-d4e9-47ca-8d52-3ad6752d980c" />

# Описание:

Каждые 15 минут оркестратор start_processing.py:
1. Берёт задачи из ops.file_registry со статусами NEW / PROCESSING / ERROR;
2. находит подходящий клиентский скрипт и запускает его только для конкретного id;
3. скрипт формирует отчёт и пишет его во временную папку «Итоговые отчёты» с именем, содержащим _id{ID}_;
4. оркестратор переносит готовые файлы в «Данные на загрузку» и обновляет статус записи до CREATED в базе данных; при проблемах — ERROR с пояснением в error_reason.

# Статусы в БД: NEW, PROCESSING, CREATED, ERROR, DELETE.

Краткое пояснение по статусам

NEW - новый файл.
PROCESSING - файл скрипта обрабатывающий отчет отсутствует.
CREATED - файл создан.
ERROR - скрипт обрабатывающий файл есть, но завершился с ошибкой.
DELETE - файл удален.

Примеры error_reason: NO_SCRIPT_FOUND, NO_OUTPUT_FILE, TIMEOUT, LOCKED, NO_SPACE, PATH_TOO_LONG, RETURN_CODE_X.

# Структура папок:

Python_scripts\automated_processing\

├─ start_processing.py              # оркестратор

├─ Reestr\
│   └─ new_files_registry.csv       # реестр текущего запуска (read-only)

├─ Scripts\

│   ├─ Distibutors\                 # поставщики типа "Дистрибьютор"

    │   └─ Client_01\Client_01_processing.py

    │   └─ Client_01\Client_02_processing.py
    
    │   └─ Client_01\Client_03_processing.py
   
├─ report_header\
│   └─ report_header.xlsx           # эталонная шапка (схема целевых колонок)

# Рабочие каталоги для файлов:

C:\Users\user\Desktop\Итоговые отчеты — сюда клиентские скрипты сохраняют промежуточные результаты.
C:\Users\user\Desktop\Данные на загрузку — сюда оркестратор переносит проверенные файлы для дальнейшей загрузки.

# Контракт клиентского скрипта:

Оркестратор запускает скрипт с переменными окружения
- TASK_ID — обязателен; обрабатывайте ровно эту запись;
- TASK_FILE, TASK_CLIENT, TASK_REPORT_TYPE — вспомогательные.

# Скрипт:

- читает исходник (csv, xls, xlsx), приводит поля к схеме report_header.xlsx;
- сохраняет файл в «Итоговые отчёты» с именем вида: {Client}_id{ID}_{source_basename}_{YYYYMMDD_HHMMSS}.xlsx
