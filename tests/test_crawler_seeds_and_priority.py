from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

from src.outreach.crawler import DomainLimitedCrawler, FetchResult


def _site_mapping() -> Dict[str, str]:
    return {
        "/": """
            <html><body>
              <a href="/blog">Blog</a>
              <a href="/aktualnosci">Aktualności</a>
              <a href="/kontakt">Kontakt</a>
            </body></html>
        """,
        "/cennik": """
            <html><body>
              <h1>Cennik ortodoncji</h1>
              <p>Konsultacja ortodontyczna 200 zł</p>
            </body></html>
        """,
        "/blog": "<html><body>Blog</body></html>",
        "/aktualnosci": "<html><body>News</body></html>",
        "/kontakt": "<html><body>Kontakt</body></html>",
    }


def _fake_fetcher(url: str) -> FetchResult:
    mapping = _site_mapping()
    parsed = urlparse(url)
    path = parsed.path or "/"
    html = mapping.get(path)
    if html is None:
        return FetchResult(
            url=url,
            final_url=url,
            status_code=404,
            content_type="text/html",
            text="",
            error="not_found",
            from_cache=False,
        )
    return FetchResult(
        url=url,
        final_url=url,
        status_code=200,
        content_type="text/html",
        text=html,
        error=None,
        from_cache=False,
    )


def test_crawler_seeds_pricing_paths_under_tight_limit(tmp_path: Path) -> None:
    evidence_dir = tmp_path / "pages"
    crawler = DomainLimitedCrawler(max_pages=3, refresh_web=True, cache=None)

    result = crawler.crawl("http://clinic.test/", evidence_dir, fetcher=_fake_fetcher)

    visited = {p.final_url for p in result.pages}
    assert "http://clinic.test/cennik" in visited
