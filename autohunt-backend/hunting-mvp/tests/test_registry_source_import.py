from app.integrations.mcp_source_fetcher.schemas import NormalizedItem
from app.services.registry_source_import import (
    REGISTRY_BENCH_COLUMN,
    REGISTRY_VACANCIES_COLUMN,
    extract_registry_source_candidates,
    parse_normalized_registry_row,
)


def test_parse_normalized_registry_row_reads_headers_from_row_text():
    row = parse_normalized_registry_row(
        "Какую компанию Вы представляете?: Firecode\n"
        "Ссылка на канал с вакансиями: https://t.me/firecode_jobs\n"
        "Ссылка на бенч Вашей компании: https://docs.google.com/spreadsheets/d/abc/edit#gid=0"
    )

    assert row["Какую компанию Вы представляете?"] == "Firecode"
    assert row["Ссылка на канал с вакансиями"] == "https://t.me/firecode_jobs"
    assert row["Ссылка на бенч Вашей компании"] == "https://docs.google.com/spreadsheets/d/abc/edit#gid=0"


def test_extract_registry_source_candidates_uses_only_target_columns_and_deduplicates():
    items = [
        NormalizedItem(
            text=(
                "Какую компанию Вы представляете?: Firecode\n"
                f"{REGISTRY_VACANCIES_COLUMN}: канала нет\n"
                f"{REGISTRY_BENCH_COLUMN}: https://docs.google.com/spreadsheets/d/abc/edit#gid=0"
            ),
            row_index=2,
            metadata={},
        ),
        NormalizedItem(
            text=(
                "Какую компанию Вы представляете?: itWit\n"
                f"{REGISTRY_VACANCIES_COLUMN}: https://docs.google.com/spreadsheets/d/xyz/edit#gid=0\n"
                f"{REGISTRY_BENCH_COLUMN}: https://docs.google.com/spreadsheets/d/abc/edit#gid=0"
            ),
            row_index=3,
            metadata={},
        ),
    ]

    out = extract_registry_source_candidates(items)

    assert len(out) == 2
    assert out[0].company_name == "Firecode"
    assert out[0].column_name == REGISTRY_BENCH_COLUMN
    assert out[0].source_url == "https://docs.google.com/spreadsheets/d/abc/edit#gid=0"
    assert out[1].company_name == "itWit"
    assert out[1].column_name == REGISTRY_VACANCIES_COLUMN
    assert out[1].source_url == "https://docs.google.com/spreadsheets/d/xyz/edit#gid=0"
