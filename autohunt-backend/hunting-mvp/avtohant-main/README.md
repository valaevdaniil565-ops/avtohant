# Matcher Frontend

Фронтенд, повторяющий интерфейс со скринов: тёмный dashboard, боковое меню, таблицы, карточки совпадений, экран ручного прогона, логи и настройки.

## Запуск

Нужен `Node.js 18+`.

```bash
cp .env.example .env
npm install
npm run dev
```

После запуска открой адрес, который покажет `Vite`, обычно это `http://localhost:5173`.

Для локальной интеграции backend должен быть поднят на `http://127.0.0.1:8000`.
Во frontend уже настроен Vite proxy:

- frontend: `http://127.0.0.1:5173`
- backend API: `/api` -> `http://127.0.0.1:8000`

## Доступные экраны

- `/`
- `/inbox`
- `/vacancies`
- `/bench`
- `/matches`
- `/process`
- `/logs`
- `/settings`

## Что уже работает через backend API

- live список вакансий
- live список специалистов
- live список совпадений
- live статус own bench
- ручной импорт текста
- ручной импорт URL
- ручной импорт файла

Если backend недоступен, интерфейс показывает fallback-данные и статус offline вместо падения.

## Сборка

```bash
npm run build
npm run preview
```
