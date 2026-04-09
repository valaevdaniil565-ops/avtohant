from app.integrations.mcp_source_fetcher.url_router import classify_url, extract_urls, route_urls


def test_classify_google_sheet():
    assert classify_url("https://docs.google.com/spreadsheets/d/abc/edit#gid=0") == "google_sheet"


def test_route_urls_blocks_localhost():
    accepted, errors = route_urls(["http://localhost:8080/x.csv"], allowed_domains=set())
    assert not accepted
    assert errors


def test_classify_yandex_disk_public_url():
    assert classify_url("https://disk.yandex.ru/i/abc123") == "yandex_disk_public"
    assert classify_url("https://disk.360.yandex.ru/i/Az1YZ4V0D1jx2g") == "yandex_disk_public"


def test_extract_urls_repairs_wrapped_google_sheet_url():
    text = (
        "look "
        "https://docs.google.com/spreadsheets/d/1i3E5mg4BGGYA8uarjcdOY\n"
        "aQfwbXsNFOmjc4dCz3DdRQ/edit7gid=1263570824#gid=1263570824"
    )
    urls = extract_urls(text)
    assert urls == [
        "https://docs.google.com/spreadsheets/d/1i3E5mg4BGGYA8uarjcdOYaQfwbXsNFOmjc4dCz3DdRQ/edit?gid=1263570824#gid=1263570824"
    ]
