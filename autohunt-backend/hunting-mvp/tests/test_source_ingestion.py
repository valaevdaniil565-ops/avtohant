import asyncio

from app.services.source_ingestion import build_external_ingestion_units
from app.integrations.mcp_source_fetcher.schemas import SourceFetchResult, NormalizedItem


class FakeClient:
    def fetch_url(self, url: str) -> SourceFetchResult:
        if "sheet" in url:
            return SourceFetchResult(
                ok=True,
                source_type="google_sheet",
                source_url=url,
                items=[
                    NormalizedItem(text="Role: Python\nGrade: Senior", row_index=2, metadata={}),
                    NormalizedItem(text="Role: QA\nGrade: Middle", row_index=3, metadata={}),
                ],
                metadata={
                    "import_summary": {
                        "items_count": 2,
                        "sheets_total": 1,
                        "sheets_processed": 1,
                        "sheets_skipped": 0,
                        "processed_sheets": [{"sheet_name": "Sheet 1", "rows_imported": 2, "tables_processed": 1}],
                        "skipped_sheets": [],
                        "confidence": {"high": 2, "medium": 0, "low": 0},
                    }
                },
            )
        return SourceFetchResult(ok=False, source_type="generic_url", source_url=url, error="boom")


def test_source_ingestion_builds_units():
    async def _run():
        out = await build_external_ingestion_units(
            "look https://docs.google.com/spreadsheets/d/sheet/edit",
            enabled=True,
            client=FakeClient(),
        )
        return out

    out = asyncio.run(_run())
    assert len(out.units) == 2
    assert "External source URL" in out.units[0].text
    assert out.summaries[0]["items_count"] == 2


def test_source_ingestion_preserves_table_entity_hint_and_structured_fields():
    class StructuredClient:
        def fetch_url(self, url: str) -> SourceFetchResult:
            return SourceFetchResult(
                ok=True,
                source_type="google_sheet",
                source_url=url,
                items=[
                    NormalizedItem(
                        text="Имя: Роман Р.\nРоль: Системный аналитик",
                        row_index=2,
                        metadata={
                            "entity_hint": "BENCH",
                            "structured_fields": {"name": "Роман Р.", "role": "Системный аналитик", "stack": ["Системный аналитик"]},
                            "sheet_name": "Analysts",
                            "table_index": 1,
                            "confidence": "high",
                        },
                    )
                ],
            )

    async def _run():
        return await build_external_ingestion_units(
            "https://docs.google.com/spreadsheets/d/sheet/edit",
            enabled=True,
            client=StructuredClient(),
        )

    out = asyncio.run(_run())
    assert out.units[0].source_meta["entity_hint"] == "BENCH"
    assert out.units[0].source_meta["structured_fields"]["role"] == "Системный аналитик"
    assert out.units[0].source_meta["sheet_name"] == "Analysts"
    assert out.units[0].source_meta["table_index"] == 1
    assert out.units[0].source_meta["confidence"] == "high"


def test_source_ingestion_splits_multibench_doc_lines():
    class DocClient:
        def fetch_url(self, url: str) -> SourceFetchResult:
            return SourceFetchResult(
                ok=True,
                source_type="google_doc",
                source_url=url,
                items=[
                    NormalizedItem(
                        text=(
                            "Python Иван Senior 5 лет опыта, ставка - 2100₽ /cv_AAA\n"
                            "Java Петр Middle 3 года опыта, ставка - 1700₽ /cv_BBB\n"
                            "React Мария Middle+ 4 года опыта, ставка - 1500₽ /cv_CCC"
                        ),
                        metadata={},
                    )
                ],
            )

    async def _run():
        return await build_external_ingestion_units(
            "https://docs.google.com/document/d/abc/edit",
            enabled=True,
            client=DocClient(),
        )

    out = asyncio.run(_run())
    assert len(out.units) == 3
