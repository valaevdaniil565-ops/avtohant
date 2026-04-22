# Инвентаризация текстовых сообщений

Дата фиксации: `2026-03-19`

## Область анализа

Актуальный работающий бот:
- `app/bots/manager_bot.py`

Актуальные сообщения в архив-канале:
- `app/collectors/tg_collector/collector.py`
- `app/bots/manager_bot.py`

Legacy-модуль с отдельными текстами, но сейчас не подключен в runtime:
- `app/bots/handlers.py`

## Общие правила рендера

Источник:
- `app/bots/manager_bot.py::safe_reply_text`
- `app/bots/manager_bot.py::split_telegram`

Как рендерятся сообщения сейчас:
- Все ответы бота идут plain text, без `parse_mode`.
- Длинные сообщения режутся на чанки по строкам, лимит около `3800` символов.
- Для paginated TOP используется `reply_text` плюс inline-кнопки `◀️ Назад` и `Вперёд ▶️`.
- Для системных архивных постов используется plain text.

## Базовые текстовые строительные блоки

### 1. Формат источника в выдаче

Источник:
- `app/bots/manager_bot.py::_compose_source_display`
- `app/services/own_specialists.py::_build_registry_source_meta`

Сейчас в текстах результата выводится одна из форм:

1. Файл по ссылке:
```text
Менеджер: {manager_name}; Ссылка на файл: {external_url}; Индекс: {index}
```

2. Архив-пост:
```text
Менеджер: {manager_name}; Ссылка на архив-пост: {canonical_url}; Индекс: {entity_index}
```

3. Telegram-сообщение:
```text
Менеджер: {manager_name}; Ссылка на сообщение: {canonical_url}
```

4. Только менеджер:
```text
Менеджер: {manager_name}
```

5. Наш бенч:
```text
Менеджер: -; Ссылка на файл: {own_bench_url}; Специалист: {specialist_name}
```

### 2. Формат строки в TOP-выдаче

Источник:
- `app/bots/manager_bot.py::format_top10`
- `app/bots/manager_bot.py::format_hits_page`

Шаблон одной строки:
```text
{rank:02d}) {percent}% | {role} | {grade_or_dash} | {stack_or_dash} | {money_or_dash} | {location_or_dash}{own_mark}
   Источник: {source_display}
```

Где:
- `own_mark` = ` | own`, если `is_internal = true`
- строка `Источник: ...` добавляется только если источник есть

### 3. Блок "Наш бенч"

Источник:
- `app/bots/manager_bot.py::format_own_bench_block`

Если есть попадания:
```text
НАШ БЕНЧ:
{format_top10(own_hits)}
```

Если попаданий нет:
```text
НАШ БЕНЧ:
На нашем бенче нет подходящих специалистов
```

## Активные сообщения менеджерского бота

### 1. Старт и help

Источник:
- `app/bots/manager_bot.py::start`
- `app/bots/manager_bot.py::help_cmd`

#### 1.1 `/start`
```text
Привет! Я менеджерский бот для мэтчинга.

Что умею:
• Пришли вакансию → верну ТОП-100 кандидатов (постранично 10/стр).
• Пришли кандидата/бенч → верну ТОП-100 вакансий (постранично 10/стр).
• Кнопка 'Наш бенч' → открою реестр собственных специалистов.
• Export: выгрузка БД в Excel (active/all).
```

#### 1.2 `/help`
```text
Кнопки:
• 🔍 Вакансия → ТОП кандидатов — пришли вакансию, отвечу ТОП-100 (10/стр).
• 👤 Кандидат/Бенч → ТОП вакансий — пришли кандидата/бенч, отвечу ТОП-100 (10/стр).
• 📁 Наш бенч — ссылка на реестр наших специалистов.
• 📤 Export (active)/📤 Export (all) — Excel выгрузка.
Команды: /start /help /export /export_all /delete
```

#### 1.3 Нет доступа
```text
Нет доступа.
```

### 2. Главное меню и кнопки

Источник:
- `app/bots/manager_bot.py::handle_buttons_and_text`

#### 2.1 Пустой текст
```text
Пришли текст.
```

#### 2.2 Кнопка "Назад"
```text
Возвращаюсь в главное меню.
```

#### 2.3 Кнопка "Вакансия"
```text
Ок. Пришли текст вакансии (или форвард).
```

#### 2.4 Кнопка "Кандидат/Бенч"
```text
Ок. Пришли текст кандидата/бенча (или форвард).
```

#### 2.5 Кнопка "Наш бенч"
Источник:
- `app/bots/manager_bot.py::show_own_bench_menu`

```text
Наш бенч: {current_url}
```

После этого показывается клавиатура:
- `✏️ Изменить ссылку на наш бенч`
- `🔄 Обновить наш бенч`
- `⬅️ Назад`

#### 2.6 Кнопка "Изменить ссылку на наш бенч"
```text
Пришли новую ссылку на Наш бенч.
Нажми «Назад», если передумал.
```

#### 2.7 Кнопка "Обновить наш бенч"
Успешный старт:
```text
Обновляю Наш бенч по текущей ссылке...
```

Ошибка:
```text
Не удалось обновить Наш бенч: {ExceptionType}
```

Успех:
```text
Наш бенч обновлён.
Текущая ссылка: {current_url}
```

#### 2.8 Изменение ссылки на "Наш бенч"
Невалидная ссылка:
```text
Нужна полная ссылка вида https://...
```

Ссылка не изменилась:
```text
Ссылка не изменилась.
Наш бенч: {current_url}
```

Процесс замены:
```text
Меняю ссылку и мягко деактивирую старый Наш бенч...
```

Ошибка новой ссылки с rollback:
```text
Новая ссылка не загрузилась: {ExceptionType}. Вернул прежнюю ссылку.
```

Успех:
```text
Ссылка на Наш бенч обновлена.
Старая ссылка деактивирована: {deactivated_count}
Новая ссылка: {candidate_url}
```

#### 2.9 Export
Источник:
- `app/bots/manager_bot.py::do_export`

```text
Готовлю Excel выгрузку ({label})…
```

Где `label` = `active` или `all`

### 3. Удаление по ссылке

Источник:
- `app/bots/manager_bot.py::delete_cmd`
- `app/bots/manager_bot.py::do_delete_by_link`
- `app/bots/manager_bot.py::on_delete_callback`

Примечание:
- Кнопка удаления сейчас убрана из основного интерфейса, но команда `/delete` ещё жива.

#### 3.1 Запрос ссылки
```text
Пришли ссылку t.me/... (можно просто сообщением).
```

или

```text
Пришли ссылку вида https://t.me/.../123 или https://t.me/c/.../123
```

#### 3.2 Некорректная ссылка
```text
Не похоже на t.me ссылку. Пример: https://t.me/c/123456/789
```

#### 3.3 Ничего не найдено
```text
Не нашёл запись по этой ссылке в sources.
```

#### 3.4 Подтверждение массового скрытия
```text
Нашёл {rows_count} записей по ссылке.
Ссылка: {url}

Скрыть ВСЕ найденные сущности (status=hidden)?
```

Кнопки:
- `✅ Подтвердить скрытие`
- `❌ Отмена`

#### 3.5 Callback-ответы
Нет активной операции:
```text
Нет активной операции удаления.
```

Отмена:
```text
Отменено.
```

Успех:
```text
Готово. Скрыто записей: {updated}.
```

### 4. Служебные статусы при разборе входящего сообщения

Источник:
- `app/bots/manager_bot.py::process_message`

#### 4.1 Переслано из чата
```text
Обнаружено пересланное сообщение из чата. Создам ссылку через архив-пост.
```

#### 4.2 Внешние ссылки обработаны
```text
Обработаны внешние ссылки:
• {url_1} -> {source_type}, items={count}
• {url_2} -> error: {reason}
Ошибки:
• {error_1}
• {error_2}
```

#### 4.3 Старт парсинга внешнего источника
```text
Начинаю парсинг внешнего источника: units={units_count}. Это может занять до 20 минут.
```

#### 4.4 Ссылки распознаны, но не прочитаны
```text
Ссылки распознаны, но прочитать их не удалось.
• {error_1}
• {error_2}
```

#### 4.5 Line-wise список бенчей
Без архивного reference-поста:
```text
Обнаружен line-wise список бенчей: lines={lines_count}. Начинаю парсинг.
```

С архивным reference-постом:
```text
Обнаружен line-wise список бенчей: lines={lines_count}. Начинаю парсинг. Для каждого релевантного блока создам ссылку через архив-пост.
```

#### 4.6 Обычный старт ручного парсинга
Без архивного reference-поста:
```text
Начинаю парсинг сообщения. Это может занять до 20 минут.
```

С архивным reference-постом:
```text
Начинаю парсинг сообщения. Создам ссылку через архив-пост. Это может занять до 20 минут.
```

#### 4.7 Классификатор решил OTHER
```text
Unit {unit_idx}/{total_units}: OTHER, пропущено.
```

#### 4.8 Не удалось извлечь вакансии
```text
Unit {unit_idx}/{total_units}: не смог извлечь вакансии.
```

#### 4.9 Не удалось извлечь специалистов
```text
Unit {unit_idx}/{total_units}: не смог извлечь специалистов.
```

#### 4.10 Не удалось создать архив-пост
```text
Не удалось создать архив-пост. Источник сохраню без ссылки на архив.
```

#### 4.11 Тип не понят
```text
Unit {unit_idx}/{total_units}: не понял тип сообщения.
```

#### 4.12 Итог по внешним ссылкам
```text
Итог: сохранено вакансий={vacancy_count}, кандидатов={specialist_count}, ошибок ссылок={errors_count}
```

### 5. Сообщения результата: вакансия -> кандидаты

Источник:
- `app/bots/manager_bot.py::process_message`
- `app/bots/manager_bot.py::send_manual_top_paginated`

#### 5.1 Header карточки вакансии
```text
Вакансия unit {unit_idx}/{total_units}, item {item_idx}/{items_count}
{role_or_unknown} | {grade_or_dash} | {stack_preview}
```

#### 5.2 Если вакансия закрыта
```text
Вакансия unit {unit_idx}/{total_units}, item {item_idx}/{items_count}
{role_or_unknown} | {grade_or_dash} | {stack_preview}
⚠️ Закрыта. Сохранил как closed.
```

#### 5.3 Полный manual TOP с пагинацией
Общий шаблон страницы:
```text
ТОП-10 кандидатов для вакансии
{header}
{summary}
{own_bench_block}
Страница {page}/{total_pages} • всего {total_hits}
Источник: {source_display}

{body}
```

Где:
- `{own_bench_block}` это либо блок с найденными своими специалистами, либо текст:
```text
НАШ БЕНЧ:
На нашем бенче нет подходящих специалистов
```

#### 5.4 Если есть TOP, но нет внутренних специалистов
Перед обычным `format_top10(top_hits)` добавляется:
```text
Собственные подходящие специалисты на текущий момент отсутствуют. Ниже показаны внешние специалисты.
```

#### 5.5 Non-paginated режим
```text
{header}
{summary}
{own_bench_block}

{top10_text}
```

### 6. Сообщения результата: бенч -> вакансии

Источник:
- `app/bots/manager_bot.py::process_message`
- `app/bots/manager_bot.py::send_manual_top_paginated`

#### 6.1 Header карточки кандидата
```text
Кандидат unit {unit_idx}/{total_units}, item {item_idx}/{items_count}
{role_or_unknown} | {grade_or_dash} | {stack_preview}
```

#### 6.2 Если специалист недоступен
```text
Кандидат unit {unit_idx}/{total_units}, item {item_idx}/{items_count}
{role_or_unknown} | {grade_or_dash} | {stack_preview}
⚠️ Не доступен. Сохранил как hired.
```

#### 6.3 Manual TOP с пагинацией
```text
ТОП-10 вакансий для кандидата
{header}
{summary}
Страница {page}/{total_pages} • всего {total_hits}
Источник: {source_display}

{body}
```

#### 6.4 Non-paginated режим
```text
{header}
{summary}

{top10_text}
```

Если вакансий нет:
```text
На данный момент в Базе нет подходящих вакансий.
```

### 7. Рассылка менеджерам из ingest-чатов

Источник:
- `app/bots/manager_bot.py::notify_managers_top10`

Используется только при auto-ingest из ingest-чатов.

Общий шаблон:
```text
{title}
{header}
{summary}
{intro_text}
Источник: {source_display}

{top10_text}
```

Текущие заголовки:
- `ТОП-10 для новой вакансии`
- `ТОП-10 для нового кандидата`

### 8. Пагинация

Источник:
- `app/bots/manager_bot.py::on_top_page_callback`

Alert при устаревшей сессии:
```text
Сессия выдачи устарела. Запроси TOP заново.
```

Alert при ошибке редактирования:
```text
Не удалось обновить страницу.
```

### 9. Daily digest

Источник:
- `app/bots/manager_bot.py::build_daily_digest_text`
- `app/bots/manager_bot.py::_format_digest_items`

#### 9.1 Пустой дайджест
```text
Дайджест за последние 24 часа
Окно: {window_start_ddmm_hhmm} - {window_end_ddmm_hhmm} МСК

Новых или обновленных вакансий и bench нет.
```

#### 9.2 Непустой дайджест
```text
Дайджест за последние 24 часа
Окно: {window_start_ddmm_hhmm} - {window_end_ddmm_hhmm} МСК

Новые вакансии
{vacancy_items_or_Нет изменений.}

Обновленные вакансии
{vacancy_items_or_Нет изменений.}

Новые bench
{bench_items_or_Нет изменений.}

Обновленные bench
{bench_items_or_Нет изменений.}
```

Шаблон одного элемента внутри секции:
```text
{idx}. {role_or_unknown} | {grade_or_dash} | {stack_or_dash} | {stamp_local}
   Источник: {source_display}
```

### 10. Глобальная ошибка

Источник:
- `app/bots/manager_bot.py::on_error`

```text
⚠️ Ошибка: {context.error}
```

## Сообщения в архив-канале

### 1. Архивный пост collector для chat-sources

Источник:
- `app/collectors/tg_collector/collector.py::_build_archive_post_text`

Когда создаётся:
- Collector читает source chat.
- Если сообщение из `channel`, архивный пост не создаётся.
- Если сообщение из `chat` и классифицируется как `bench` или `vacancy`, создаётся архивный пост.

Шаблон:
```text
Источник: {chat_title} / @{chat_username} / {chat_id}
Дата исходного сообщения: {yyyy-mm-dd hh:mm:ss} UTC
Отправитель: @{sender_username} ({sender_name}) / {sender_id}
Тип: {classification}
Ссылка: {original_message_url}

-- копия исходного сообщения --
{raw_text}
```

Варианты fallback:
- если нет `chat_username`, соответствующая часть может отсутствовать
- если нет `sender_username`, используется имя
- если нет имени, будет `Отправитель: unknown`
- если нет даты, будет `Дата исходного сообщения: unknown`
- если нет текста, будет `[без текста]`

Технические детали:
- Полный пост обрезается до `3900` символов.
- `classification` сейчас только `bench` или `vacancy`.

### 2. Reference-only архив-пост от manager_bot

Источник:
- `app/bots/manager_bot.py::_build_reference_archive_text`

Когда создаётся:
- менеджер прислал обычный текст без внешних ссылок и без `t.me`
- менеджер переслал сообщение из чата

Не создаётся:
- для внешних файлов
- для пересланных сообщений из каналов
- для архивного ingest-режима

Шаблон:
```text
Дата исходного сообщения: {yyyy-mm-dd hh:mm:ss} UTC
Источник: {source_name}
Менеджер: {manager_name}
Режим: reference-only

-- копия исходного сообщения --
{original_text}

-- индексы сущностей --
1. {role_1} | {grade_1} | {stack_1}
2. {role_2} | {grade_2} | {stack_2}
...
```

Назначение:
- этот пост нужен как канонический source link
- bot не ingest-ит такие посты как сущности повторно

### 3. Ручные сообщения в архив-канале

Источник:
- `app/collectors/tg_collector/collector.py`, ветка `source == "archive_manual"`

Шаблон:
- системного шаблона нет
- collector просто сохраняет raw
- manager_bot читает такие сообщения напрямую из archive chat и обрабатывает их как обычный текст

## Отдельно: что именно сейчас считается "красивым текстом результата"

Самые важные визуально насыщенные шаблоны, которые пользователь реально видит чаще всего:
- стартовое меню
- страница TOP-выдачи
- блок `НАШ БЕНЧ`
- daily digest
- архивный collector post
- reference-only archive post

Именно эти 6 форматов лучше всего переработать в первую очередь.

## Legacy / неактивные шаблоны

Источник:
- `app/bots/handlers.py`

Статус:
- в текущем запуске не используется
- runtime стартует через `python -m app.bots.manager_bot`

Но в проекте остаются старые тексты:

### Legacy start
```text
Я готов.
Пришли текст вакансии/бенча (можно пересланное сообщение).
Команды: /id, /export
```

### Legacy id
```text
chat_id={chat_id}
user_id={user_id}
```

### Legacy export
```text
Экспорт подключим следующим шагом (openpyxl).
```

### Legacy callback / errors
```text
Ошибка конфигурации: settings не переданы в bot_data.
Некорректная команда.
Скрыто: {entity_type} {entity_id}
Неизвестная команда.
Ошибка: {context.error}
```

### Legacy ACL и приём текста
```text
ACL parse error: {error}
MANAGER_CHAT_IDS должен быть числами через запятую.
```

```text
ACL blocked. chat_id={chat_id} not in {allowed_ids}
```

```text
Пришли текст (сообщение должно содержать текст).
```

```text
Принял. Обрабатываю…
```

## Практические выводы для будущего редизайна

1. Сейчас один и тот же смысл рендерится несколькими стилями:
- start/help
- TOP page
- notify payload
- digest
- archive posts

2. Почти все тексты формируются строковой склейкой внутри `manager_bot.py`, а не через единый слой шаблонов.

3. Для редизайна лучше сначала выделить отдельные view-builder’ы:
- `render_top_page`
- `render_digest`
- `render_archive_post`
- `render_reference_archive_post`
- `render_status_message`

4. Для улучшения читаемости сильнее всего помогут:
- единые заголовки
- единые маркеры секций
- сокращение повторяющихся `unit/item`
- единый формат источника
- более ровная типографика блоков `НАШ БЕНЧ`, `Источник`, `Совпадение`, `Ставка`, `Локация`
