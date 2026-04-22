from sqlalchemy import text
from app.config import get_settings
from app.db.engine import make_engine

def main():
    s = get_settings()
    engine = make_engine(s.DATABASE_URL)
    with engine.connect() as c:
        # pgvector ext
        c.execute(text("SELECT extname FROM pg_extension WHERE extname='vector'"))
        # tables
        tables = c.execute(text("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname='public'
            ORDER BY tablename
        """)).fetchall()
        print("tables:", [t[0] for t in tables])

        # quick sanity: count indexes on embeddings
        idx = c.execute(text("""
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname='public'
              AND indexname IN ('idx_vacancies_embedding','idx_specialists_embedding')
        """)).fetchall()
        print("vector_indexes:", [i[0] for i in idx])

    print("DB OK")

if __name__ == "__main__":
    main()
