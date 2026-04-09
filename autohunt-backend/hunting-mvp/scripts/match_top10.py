import sys
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from app.config import get_settings
from app.db.engine import make_engine

TOP_K_DEFAULT = 10


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--vacancy-id", type=str, default=None)
    p.add_argument("--specialist-id", type=str, default=None)
    p.add_argument("--top-k", type=int, default=TOP_K_DEFAULT)
    p.add_argument("--save", action="store_true", help="Save results to matches table")
    return p.parse_args()


def main():
    args = parse_args()
    if args.vacancy_id and args.specialist_id:
        print("Choose only one: --vacancy-id OR --specialist-id")
        return

    s = get_settings()
    engine = make_engine(s.DATABASE_URL)
    top_k = args.top_k

    with engine.begin() as c:
        # =========================
        # vacancy -> specialists (default)
        # =========================
        if not args.specialist_id:
            if args.vacancy_id:
                vac = c.execute(
                    text("""
                        SELECT id, role, embedding
                        FROM vacancies
                        WHERE id = CAST(:id AS uuid)
                    """),
                    {"id": args.vacancy_id},
                ).fetchone()
            else:
                vac = c.execute(
                    text("""
                        SELECT id, role, embedding
                        FROM vacancies
                        WHERE status='active' AND embedding IS NOT NULL
                        ORDER BY created_at DESC
                        LIMIT 1
                    """)
                ).fetchone()

            if not vac:
                print("No vacancy found (or no embedding).")
                return

            vac_id, vac_role, vac_emb = vac[0], vac[1], vac[2]
            if vac_emb is None:
                print("Vacancy has no embedding.")
                return

            print(f"Vacancy: {vac_id} | {vac_role}")

            rows = c.execute(
                text(f"""
                    SELECT
                      s.id,
                      s.role,
                      s.location,
                      (1 - (s.embedding <=> :emb)) AS similarity
                    FROM specialists s
                    WHERE s.status='active' AND s.embedding IS NOT NULL
                    ORDER BY s.embedding <=> :emb
                    LIMIT {top_k}
                """),
                {"emb": vac_emb},
            ).fetchall()

            if not rows:
                print("No specialists with embedding.")
                return

            for i, r in enumerate(rows, 1):
                print(f"{i:02d}. {r[1]} | {r[2]} | similarity={float(r[3]):.4f} | id={r[0]}")

            if args.save:
                for rank, r in enumerate(rows, 1):
                    spec_id = r[0]
                    sim = float(r[3])
                    c.execute(
                        text("""
                            INSERT INTO matches(vacancy_id, specialist_id, similarity_score, rank)
                            VALUES (:vacancy_id, :specialist_id, :score, :rank)
                            ON CONFLICT (vacancy_id, specialist_id)
                            DO UPDATE SET similarity_score=EXCLUDED.similarity_score,
                                          rank=EXCLUDED.rank,
                                          updated_at=NOW()
                        """),
                        {"vacancy_id": vac_id, "specialist_id": spec_id, "score": sim, "rank": rank},
                    )
                print(f"Saved {len(rows)} matches for vacancy {vac_id}")

        # =========================
        # specialist -> vacancies
        # =========================
        else:
            if args.specialist_id:
                spec = c.execute(
                    text("""
                        SELECT id, role, embedding
                        FROM specialists
                        WHERE id = CAST(:id AS uuid)
                    """),
                    {"id": args.specialist_id},
                ).fetchone()
            else:
                spec = c.execute(
                    text("""
                        SELECT id, role, embedding
                        FROM specialists
                        WHERE status='active' AND embedding IS NOT NULL
                        ORDER BY created_at DESC
                        LIMIT 1
                    """)
                ).fetchone()

            if not spec:
                print("No specialist found (or no embedding).")
                return

            spec_id, spec_role, spec_emb = spec[0], spec[1], spec[2]
            if spec_emb is None:
                print("Specialist has no embedding.")
                return

            print(f"Specialist: {spec_id} | {spec_role}")

            rows = c.execute(
                text(f"""
                    SELECT
                      v.id,
                      v.role,
                      v.location,
                      (1 - (v.embedding <=> :emb)) AS similarity
                    FROM vacancies v
                    WHERE v.status='active' AND v.embedding IS NOT NULL
                    ORDER BY v.embedding <=> :emb
                    LIMIT {top_k}
                """),
                {"emb": spec_emb},
            ).fetchall()

            if not rows:
                print("No vacancies with embedding.")
                return

            for i, r in enumerate(rows, 1):
                print(f"{i:02d}. {r[1]} | {r[2]} | similarity={float(r[3]):.4f} | id={r[0]}")

            if args.save:
                for rank, r in enumerate(rows, 1):
                    vac_id = r[0]
                    sim = float(r[3])
                    c.execute(
                        text("""
                            INSERT INTO matches(vacancy_id, specialist_id, similarity_score, rank)
                            VALUES (:vacancy_id, :specialist_id, :score, :rank)
                            ON CONFLICT (vacancy_id, specialist_id)
                            DO UPDATE SET similarity_score=EXCLUDED.similarity_score,
                                          rank=EXCLUDED.rank,
                                          updated_at=NOW()
                        """),
                        {"vacancy_id": vac_id, "specialist_id": spec_id, "score": sim, "rank": rank},
                    )
                print(f"Saved {len(rows)} matches for specialist {spec_id}")


if __name__ == "__main__":
    main()
