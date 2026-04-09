from app.bots.manager_bot import _build_structured_specialist_item, _build_structured_vacancy_item


def test_build_structured_specialist_item_sanitizes_role_blob_and_splits_stack():
    fields = {
        "role": (
            "Стек:\n"
            "Langs: Javascript, Typescript, node js\n"
            "Databases: Postgresql, MySql, Mongodb\n"
            "Devops: docker, nginx\n"
            "Stack: React.js, Next.js, Redux"
        ),
        "stack": [
            "Стек:\nLangs: Javascript, Typescript, node js\nDatabases: Postgresql, MySql, Mongodb",
            "Devops: docker, nginx",
            "Stack: React.js, Next.js, Redux",
        ],
        "grade": "Senior",
        "currency": "RUB",
        "location": "РФ, СПБ",
    }
    raw = "Frontend Разработчик\nКирилл З.\nMiddle+\nLangs: Javascript, Typescript, node js"

    item = _build_structured_specialist_item(fields, raw)

    assert "Langs:" not in item["role"]
    assert len(item["role"]) <= 255
    assert "Javascript" in item["stack"]
    assert "Typescript" in item["stack"]
    assert "node js" in item["stack"]
    assert "docker" in item["stack"]


def test_build_structured_vacancy_item_sanitizes_blob_role():
    fields = {
        "role": "Stack: Java, Spring Boot, Kafka, PostgreSQL, Docker, Kubernetes, AWS",
        "stack": "Java, Spring Boot, Kafka, PostgreSQL, Docker, Kubernetes, AWS",
        "company": "Acme",
    }
    raw = "Senior Java Developer\nSpring Boot, Kafka, PostgreSQL\nAcme"

    item = _build_structured_vacancy_item(fields, raw)

    assert "Stack:" not in item["role"]
    assert item["role"] != fields["role"]
    assert "Java" in item["stack"]


def test_build_structured_specialist_item_uses_stack_when_role_is_url_and_extracts_name():
    fields = {
        "role": "https://docs.google.com/spreadsheets/d/example/edit?gid=0#gid=0",
        "stack": ["React.js"],
        "grade": "Middle",
    }
    raw = (
        "https://docs.google.com/spreadsheets/d/example/edit?gid=0#gid=0\n\n"
        "---\n"
        "Имя: Кирилл З.\n"
        "Стек: React.js\n"
        "Грейд: Middle"
    )

    item = _build_structured_specialist_item(fields, raw)

    assert item["role"] == "React.js"
    assert item["name"] == "Кирилл З."


def test_build_structured_specialist_item_keeps_stack_phrase_without_comma_as_single_value():
    fields = {
        "role": "QA Engineer",
        "stack": "Авто QA Python",
        "grade": "Middle",
    }

    item = _build_structured_specialist_item(fields, "QA Engineer\nАвто QA Python\nMiddle")

    assert item["stack"] == ["Авто QA Python"]
