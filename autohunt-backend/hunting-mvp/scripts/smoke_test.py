import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def main():
    url = os.environ["DATABASE_URL"]
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
        v = conn.execute(text("SELECT extname FROM pg_extension WHERE extname='vector'")).scalar()
        print("pgvector:", v)
        print("ok")

if __name__ == "__main__":
    main()
