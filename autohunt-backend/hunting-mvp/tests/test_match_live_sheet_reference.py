from app.integrations.mcp_source_fetcher.schemas import NormalizedItem, SourceFetchResult
from app.use_cases.matching import _resolve_live_sheet_reference


class FakeFetcher:
    def fetch_url(self, url: str) -> SourceFetchResult:
        return SourceFetchResult(
            ok=True,
            source_type="google_sheet",
            source_url=url,
            items=[
                NormalizedItem(
                    text="Имя: Кирилл И.",
                    row_index=3,
                    metadata={
                        "sheet_name": "PHP",
                        "table_index": 1,
                        "row_map": {"name": "Кирилл И.", "role": "PHP", "location": "Саранск"},
                        "structured_fields": {"name": "Кирилл И.", "role": "PHP", "location": "Саранск"},
                    },
                ),
                NormalizedItem(
                    text="Имя: Даниил П.",
                    row_index=8,
                    metadata={
                        "sheet_name": "PHP",
                        "table_index": 1,
                        "row_map": {
                            "name": "Даниил П.",
                            "role": "PHP",
                            "location": "Москва",
                            "resume_url": "https://docs.google.com/document/d/daniil/edit",
                        },
                        "structured_fields": {
                            "name": "Даниил П.",
                            "role": "PHP",
                            "location": "Москва",
                            "resume_url": "https://docs.google.com/document/d/daniil/edit",
                        },
                    },
                ),
            ],
        )


def test_resolve_live_sheet_reference_prefers_current_row_by_resume_and_name():
    item = {
        "specialist_source_url": "https://docs.google.com/spreadsheets/d/example/edit#gid=1",
        "specialist_source_meta": {
            "manager_name": "@Le0kz",
            "source_kind": "file",
            "row_map": {
                "name": "Даниил П.",
                "role": "PHP",
                "location": "Москва",
                "resume_url": "https://docs.google.com/document/d/daniil/edit",
            },
            "structured_fields": {
                "name": "Даниил П.",
                "role": "PHP",
                "location": "Москва",
                "resume_url": "https://docs.google.com/document/d/daniil/edit",
            },
        },
        "specialist_original_text": "Имя: Даниил П.\nРоль: PHP\nЛокация: Москва",
        "specialist_role": "PHP",
        "specialist_location": "Москва",
    }

    resolved = _resolve_live_sheet_reference(item, source_fetcher=FakeFetcher(), cache={})

    assert resolved["specialist_live_sheet_name"] == "PHP"
    assert resolved["specialist_live_row_index"] == 8
    assert "Строка: 8" in resolved["specialist_live_source_display"]
