# Runbook: Очистка БД И Перезагрузка Данных

Дата: `2026-03-23`

Этот гайд нужен для нового устройства или нового окружения, где:
- в БД остались данные от старых версий проекта;
- нужно полностью очистить БД;
- нужно заново загрузить данные уже через текущий бот и текущий формат импорта.

## Что будет в результате

После выполнения шагов:
- БД будет пустой и инициализированной заново;
- бот будет запущен;
- `Наш бенч` будет задан и пересинхронизирован;
- вакансии и бенчи можно будет загружать через новые кнопки бота;
- при необходимости можно будет сделать массовую загрузку из реестра ссылок через скрипт.

## 1. Подготовка проекта

В корне проекта:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Проверка `.env`

Минимально должны быть настроены:

```env
DATABASE_URL=postgresql+psycopg://hunting:hunting@127.0.0.1:5432/hunting
ENABLE_EXTERNAL_LINK_INGEST=true
MCP_SOURCE_FETCHER_TRANSPORT=stdio
MCP_SOURCE_FETCHER_COMMAND=python -m app.integrations.mcp_source_fetcher.server
SOURCE_FETCHER_MAX_BYTES=25000000
```

Для запуска менеджерского бота также должны быть заданы его Telegram-переменные, которые уже используются в проекте:
- `BOT_TOKEN`
- `MANAGER_CHAT_IDS`

Если позже понадобится массовая загрузка через collector-аккаунт, дополнительно нужны:

```env
TG_API_ID=...
TG_API_HASH=...
TG_SESSION_NAME=storage/sessions/collector
```

## 3. Очистить Только Данные, Не Трогая Схему

Это основной рекомендуемый сценарий.

Он:
- не удаляет Docker volume;
- не требует повторной инициализации схемы;
- оставляет системные настройки и справочники;
- удаляет старые вакансии, специалистов, мэтчи и связанные source-данные от предыдущих версий.

Важно:
- мы чистим не только `vacancies`, `specialists` и `matches`;
- мы также чистим связанные таблицы `sources`, `tg_archive_map` и `own_specialists_registry`;
- иначе останутся сиротские ссылки, старые архивные записи и хвосты `Наш бенч`.

### Шаг 3.1. Поднять БД

```bash
docker compose up -d db
```

### Шаг 3.2. Проверить, что схема уже существует

```bash
source .venv/bin/activate
PYTHONPATH=. python scripts/db_check.py
```

Ожидаемый результат:

```text
DB OK
```

### Шаг 3.3. Очистить только данные проекта

```bash
psql postgresql://hunting:hunting@127.0.0.1:5432/hunting -c "
TRUNCATE TABLE
  matches,
  sources,
  tg_archive_map,
  own_specialists_registry,
  vacancies,
  specialists
RESTART IDENTITY;
"
```

Что останется нетронутым:
- `app_settings`
- `channels`
- `partner_companies`
- `entity_aliases`

### Шаг 3.4. Проверить, что данные действительно удалены

```bash
psql postgresql://hunting:hunting@127.0.0.1:5432/hunting -c "
SELECT 'vacancies' AS table_name, count(*) FROM vacancies
UNION ALL
SELECT 'specialists', count(*) FROM specialists
UNION ALL
SELECT 'sources', count(*) FROM sources
UNION ALL
SELECT 'matches', count(*) FROM matches
UNION ALL
SELECT 'own_specialists_registry', count(*) FROM own_specialists_registry
UNION ALL
SELECT 'tg_archive_map', count(*) FROM tg_archive_map;
"
```

Ожидаемо: все значения должны быть `0`.

### Шаг 3.5. Когда всё-таки нужен полный reset через volume

Полный reset через `docker compose down -v` нужен только если:
- схема в БД уже сильно разъехалась;
- есть подозрение на битые миграции;
- нужно полностью снести не только данные, но и служебные настройки.

Тогда используйте старый вариант:

```bash
docker compose down -v
docker compose up -d db
source .venv/bin/activate
psql postgresql://hunting:hunting@127.0.0.1:5432/hunting -f scripts/init_db.sql
PYTHONPATH=. python scripts/db_check.py
```

## 4. Поднять бота

Откройте отдельный терминал и запустите:

```bash
cd /путь/к/hunting-mvp
source .venv/bin/activate
PYTHONPATH=. python -m app.bots.manager_bot
```

Этот терминал не закрывать.

## 5. Что нажимать в боте сначала

После запуска бота:

1. Откройте чат с ботом.
2. Нажмите `/start`.
3. Нажмите `📁 Наш бенч`.
4. Нажмите `✏️ Изменить ссылку на наш бенч`.
5. Отправьте новую публичную ссылку на актуальный файл `Наш бенч`.

Подходят публичные источники, которые бот умеет читать:
- Google Sheets
- `.xlsx`
- `.csv`
- `.pdf`
- `.docx`
- `.txt`
- другие поддерживаемые публичные file-based ссылки

После этого бот должен ответить сообщением вида:

```text
Ссылка на наш бенч обновлена
```

Далее:

6. Нажмите `🔄 Обновить наш бенч`.
7. Дождитесь ответа:

```text
Наш бенч обновлён
```

Только после этого переходите к загрузке вакансий и внешних бенчей.

## 6. Как загружать данные вручную через бота

### 6.1. Загрузка бенчей

В боте:

1. Нажмите `📥 Загрузить бенч`.
2. Отправьте один источник:
   - ссылку на публичный файл;
   - сам файл вложением;
   - или текст бенча.

Важно:
- отправляйте по одному источнику за раз;
- дождитесь, пока бот закончит обработку;
- затем отправляйте следующий источник.

### 6.2. Загрузка вакансий

В боте:

1. Нажмите `📥 Загрузить вакансии`.
2. Отправьте один источник:
   - ссылку на публичный файл;
   - сам файл вложением;
   - или текст вакансии.

Важно:
- не используйте для импорта кнопки `🔍 Вакансия → ТОП кандидатов` и `👤 Кандидат/Бенч → ТОП вакансий`;
- эти кнопки нужны для поиска и мэтчинга, а не для загрузки базы.

## 7. Рекомендуемый порядок ручной загрузки

На новом окружении лучше идти так:

1. Очистить БД.
2. Поднять бота.
3. Задать актуальную ссылку `Наш бенч`.
4. Нажать `🔄 Обновить наш бенч`.
5. Загрузить 1 тестовый файл через `📥 Загрузить бенч`.
6. Загрузить 1 тестовый файл через `📥 Загрузить вакансии`.
7. Проверить в боте, что карточки создаются корректно.
8. Только потом массово загружать все остальные источники.

## 8. Массовая загрузка из registry-файла со ссылками

Если есть реестр со ссылками на file-based источники, можно загрузить их автоматически скриптом:

```bash
scripts/import_registry_sources_via_bot.py
```

Скрипт:
- читает registry Google Sheet;
- извлекает ссылки из колонок с вакансиями и бенчами;
- валидирует ссылки;
- пишет в бота по одной ссылке;
- ждёт завершения обработки;
- отправляет следующую ссылку.

### Шаг 8.1. Один раз авторизовать collector Telegram session

На новом устройстве:

```bash
source .venv/bin/activate
python scripts/tg_login_qr.py
```

Дальше:
- откройте Telegram на телефоне;
- зайдите в `Settings → Devices → Link Desktop Device`;
- отсканируйте QR;
- дождитесь сообщения об успешной авторизации.

### Шаг 8.2. Тестовый прогон на нескольких ссылках

Пока бот запущен:

```bash
source .venv/bin/activate
PYTHONPATH=. python scripts/import_registry_sources_via_bot.py --bot-username <username_бота> --dry-run --limit 3
```

### Шаг 8.3. Боевой прогон

```bash
source .venv/bin/activate
PYTHONPATH=. python scripts/import_registry_sources_via_bot.py --bot-username <username_бота>
```

Если у бота нет username, можно использовать его numeric id:

```bash
PYTHONPATH=. python scripts/import_registry_sources_via_bot.py --bot-id <bot_id>
```

## 9. Нужно ли поднимать collector

Для ручной перезаливки БД collector не обязателен.

Если задача именно:
- очистить БД;
- заново залить актуальные данные в новом формате;

то достаточно:
- поднять БД;
- поднять `manager_bot`;
- загрузить `Наш бенч`;
- загрузить источники через кнопки бота или через `import_registry_sources_via_bot.py`.

Collector нужен только если вы хотите снова включить постоянный auto-ingest из Telegram-чатов/каналов.

Тогда его запускать отдельно:

```bash
source .venv/bin/activate
PYTHONPATH=. python -m app.collectors.tg_collector
```

Рекомендация:
- сначала завершить полную ручную перезаливку;
- только потом включать collector.

## 10. Быстрая проверка после загрузки

После загрузки данных проверьте в боте:

1. Нажмите `🔍 Вакансия → ТОП кандидатов`.
2. Отправьте тестовую вакансию, например:

```text
нужен Backend Developer Senior Java
```

Проверьте:
- есть ли внешние кандидаты;
- появился ли блок `НАШ БЕНЧ`, если в `Нашем бенче` есть релевантный специалист.

Потом:

1. Нажмите `👤 Кандидат/Бенч → ТОП вакансий`.
2. Отправьте тестовый профиль кандидата или короткий бенч-текст.

Проверьте:
- строится ли TOP вакансий;
- корректно ли выглядит источник;
- нет ли ошибок парсинга.

## 11. Если нужно всё остановить после загрузки

Если бот и collector были запущены в отдельных терминалах, в каждом из них нажмите:

```text
Ctrl+C
```

Если нужно остановить и БД:

```bash
docker compose down
```

Если нужно остановить БД и удалить данные:

```bash
docker compose down -v
```

## 12. Короткая версия по шагам

### Терминал 1

```bash
cd /путь/к/hunting-mvp
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d db
PYTHONPATH=. python scripts/db_check.py
psql postgresql://hunting:hunting@127.0.0.1:5432/hunting -c "
TRUNCATE TABLE
  matches,
  sources,
  tg_archive_map,
  own_specialists_registry,
  vacancies,
  specialists
RESTART IDENTITY;
"
PYTHONPATH=. python -m app.bots.manager_bot
```

### В боте

1. `/start`
2. `📁 Наш бенч`
3. `✏️ Изменить ссылку на наш бенч`
4. отправить ссылку на актуальный файл `Наш бенч`
5. `🔄 Обновить наш бенч`
6. `📥 Загрузить бенч` → отправлять бенчи
7. `📥 Загрузить вакансии` → отправлять вакансии

### Для массовой загрузки

```bash
source .venv/bin/activate
python scripts/tg_login_qr.py
PYTHONPATH=. python scripts/import_registry_sources_via_bot.py --bot-username <username_бота> --dry-run --limit 3
PYTHONPATH=. python scripts/import_registry_sources_via_bot.py --bot-username <username_бота>
```
