# Avtohant

Monorepo with two main applications:

- `autohunt-frondend` - React/Vite frontend
- `autohunt-backend/hunting-mvp` - backend API, worker, scheduler, Telegram bot and collector

Local setup details live inside each project folder.

## Deployment

Vercel hosts only the React/Vite frontend from `autohunt-frondend`. The FastAPI backend needs a separate always-on service with PostgreSQL/pgvector because it creates DB tables on startup and uses background jobs, Telegram integrations and external source ingestion.

Recommended production setup:

1. Deploy `autohunt-backend/hunting-mvp` as a Python web service on Render, Railway, Fly.io or another backend host.
2. Attach PostgreSQL with pgvector and set the backend `DATABASE_URL`.
3. Set backend `BACKEND_CORS_ORIGINS` to the Vercel frontend URL, for example `https://your-project.vercel.app`.
4. In Vercel project settings, set frontend `VITE_API_BASE_URL` to the backend URL, for example `https://your-backend.onrender.com`.
5. Redeploy Vercel after changing `VITE_API_BASE_URL`, because Vite bakes this value into the build.
