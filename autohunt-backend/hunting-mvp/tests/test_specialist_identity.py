from app.db.repo import generate_specialist_synthetic_id


def test_specialist_synthetic_id_prefers_resume_url():
    first = generate_specialist_synthetic_id(
        {
            "name": "Спартак А.",
            "role": "Java",
            "location": "Владикавказ",
            "resume_url": "https://docs.google.com/document/d/19gzap90a3nfnu-qxM8EQAvyWPrsHIsHv/edit",
            "stack": ["Java"],
        }
    )
    second = generate_specialist_synthetic_id(
        {
            "name": "Максим М.",
            "role": "Java",
            "location": "Москва",
            "resume_url": "https://docs.google.com/document/d/10Cg20meInD2tyoyOStlhukdpPUmnzAJI/edit",
            "stack": ["Java"],
        }
    )

    assert first != second


def test_specialist_synthetic_id_uses_name_role_location_when_resume_missing():
    first = generate_specialist_synthetic_id(
        {
            "name": "Даниил П.",
            "role": "PHP",
            "location": "Москва",
            "stack": ["PHP"],
        }
    )
    second = generate_specialist_synthetic_id(
        {
            "name": "Кирилл И.",
            "role": "PHP",
            "location": "Саранск",
            "stack": ["PHP"],
        }
    )

    assert first != second


def test_specialist_synthetic_id_uses_source_urls_when_resume_url_missing():
    first = generate_specialist_synthetic_id(
        {
            "name": "Спартак А.",
            "role": "Java",
            "location": "Владикавказ",
            "source_urls": ["https://docs.google.com/document/d/19gzap90a3nfnu-qxM8EQAvyWPrsHIsHv/edit"],
            "stack": ["Java"],
        }
    )
    second = generate_specialist_synthetic_id(
        {
            "name": "Максим М.",
            "role": "Java",
            "location": "Москва",
            "source_urls": ["https://docs.google.com/document/d/10Cg20meInD2tyoyOStlhukdpPUmnzAJI/edit"],
            "stack": ["Java"],
        }
    )

    assert first != second
