from app.services.partner_companies import detect_partner_company_mention, extract_partner_company_counts_from_sheet


class _FakeItem:
    def __init__(self, text: str):
        self.text = text


class _FakeRes:
    def __init__(self, items):
        self.ok = True
        self.items = items


class _FakeClient:
    def __init__(self, res):
        self._res = res

    def fetch_url(self, url: str):
        return self._res


def test_detect_partner_company_mention():
    text_in = "Ищем Java разработчика в команду СберБизнес."
    hit = detect_partner_company_mention(text_in, ["Сбер", "Лемана Про"])
    assert hit == "Сбер"


def test_extract_partner_company_counts_from_sheet_uses_aliases():
    items = [
        _FakeItem("Описание проекта: СберБизнес, команда core"),
        _FakeItem("Заказчик: Sber CIB"),
        _FakeItem("Клиент: Лемана Про"),
    ]
    fake = _FakeClient(_FakeRes(items))
    counts = extract_partner_company_counts_from_sheet("https://example.com/sheet", fake)
    assert counts.get("Сбер") == 2
    assert counts.get("Лемана Про") == 1
