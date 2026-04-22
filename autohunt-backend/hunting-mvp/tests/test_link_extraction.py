from app.services.link_extraction import extract_external_urls


def test_extract_external_urls_filters_tme():
    text = "bench https://docs.google.com/spreadsheets/d/abc/edit and https://t.me/c/123/10"
    urls = extract_external_urls(text)
    assert "https://docs.google.com/spreadsheets/d/abc/edit" in urls
    assert all("t.me/" not in u for u in urls)
