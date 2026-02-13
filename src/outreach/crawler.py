"""Domain-limited crawler with strict safety limits."""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional
from urllib.parse import urljoin, urlparse
import re


import requests

from .extractors import (
    ExtractedLink,
    ascii_slug,
    extract_links_forms_emails_pdfs,
    is_contact_url,
    is_pricing_url,
    normalize_netloc,
    same_domain,
)

logger = logging.getLogger(__name__)

USER_AGENT = "OrthoRankerOutreachBot/1.0 (+manual-review)"
DEFAULT_TIMEOUT_SECONDS = 15
DEFAULT_DELAY_SECONDS = 0.2
MAX_RESPONSE_BYTES = 1_500_000
SEED_PATHS: List[str] = [
    "/cennik",
    "/cennik-ortodoncja",
    "/aparat-staly-cena",
    "/cennik/",
    "/cennik-uslug",
    "/cennik-uslug-stomatologicznych",
    "/ortodoncja/cennik",
    "/leczenie/cennik",
    "/cennikortodontyczny",
    "/ceny",
    "/price-list",
    "/pricing",
]
SEED_PRIORITY = 120


@dataclass(frozen=True)
class FetchResult:
    url: str
    final_url: str
    status_code: int
    content_type: str
    text: str
    error: Optional[str] = None
    from_cache: bool = False


@dataclass(frozen=True)
class CrawlPage:
    index: int
    url: str
    final_url: str
    status_code: int
    content_type: str
    text: str
    visible_text: str
    links: List[ExtractedLink]
    forms: List[str]
    mailto_emails: List[str]
    pdf_links: List[str]
    is_pricing_candidate: bool
    is_contact_candidate: bool


@dataclass(frozen=True)
class CrawlResult:
    start_url: str
    base_netloc: str
    pages: List[CrawlPage]
    visited_urls: List[str]
    errors: List[str]
    cache_hits: int
    live_fetches: int


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _hash_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _is_likely_pricing_page(url: str, text: str) -> bool:
    """Return True if the page looks like a real pricing page, not just marketing.
    
    To avoid false positives from landing pages with promo text like "konsultacja 100zł",
    we require BOTH a pricing URL pattern AND multiple price mentions.
    """
    # Must have a pricing URL pattern (cennik, ceny, pricing, etc.)
    if not is_pricing_url(url):
        return False
    
    # Count price mentions (digits followed by zł or PLN)
    # Skip marketing pages with minimal price info
    import re
    price_pattern = re.compile(r'\d[\d\s.,]*\s*(zł|pln)', re.IGNORECASE)
    price_matches = price_pattern.findall(text)
    
    # Require multiple price entries (3+) to confirm it's a real price list
    if len(price_matches) >= 3:
        return True
    
    return False


class PageCache:
    """Small file-based cache for fetched pages."""

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _paths(self, url: str) -> tuple[Path, Path]:
        key = _hash_url(url)
        return self.cache_dir / f"{key}.meta.json", self.cache_dir / f"{key}.body.txt"

    def get(self, url: str) -> Optional[FetchResult]:
        meta_path, body_path = self._paths(url)
        if not meta_path.exists() or not body_path.exists():
            return None
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            body = body_path.read_text(encoding="utf-8")
        except Exception:
            return None
        return FetchResult(
            url=url,
            final_url=str(meta.get("final_url") or url),
            status_code=int(meta.get("status_code") or 0),
            content_type=str(meta.get("content_type") or ""),
            text=body,
            error=meta.get("error"),
            from_cache=True,
        )

    def set(self, result: FetchResult) -> None:
        meta_path, body_path = self._paths(result.url)
        meta = {
            "url": result.url,
            "final_url": result.final_url,
            "status_code": result.status_code,
            "content_type": result.content_type,
            "error": result.error,
            "fetched_at": _utc_now_iso(),
        }
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        body_path.write_text(result.text or "", encoding="utf-8")


def _fetch_with_requests(url: str, session: requests.Session) -> FetchResult:
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = session.get(url, headers=headers, timeout=DEFAULT_TIMEOUT_SECONDS, stream=True)
    except requests.RequestException as exc:
        return FetchResult(
            url=url,
            final_url=url,
            status_code=0,
            content_type="",
            text="",
            error=f"request_error: {exc}",
            from_cache=False,
        )

    content_type = (resp.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
    final_url = str(resp.url)

    chunks: List[bytes] = []
    total = 0
    try:
        for chunk in resp.iter_content(chunk_size=16_384):
            if not chunk:
                continue
            chunks.append(chunk)
            total += len(chunk)
            if total >= MAX_RESPONSE_BYTES:
                break
    finally:
        resp.close()

    raw = b"".join(chunks)[:MAX_RESPONSE_BYTES]
    encoding = resp.encoding or "utf-8"
    try:
        text = raw.decode(encoding, errors="ignore")
    except LookupError:
        text = raw.decode("utf-8", errors="ignore")

    return FetchResult(
        url=url,
        final_url=final_url,
        status_code=resp.status_code,
        content_type=content_type,
        text=text,
        error=None,
        from_cache=False,
    )


FetchFn = Callable[[str], FetchResult]


class DomainLimitedCrawler:
    def __init__(
        self,
        max_pages: int = 30,
        delay_seconds: float = DEFAULT_DELAY_SECONDS,
        refresh_web: bool = False,
        cache: Optional[PageCache] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.max_pages = max(1, int(max_pages))
        self.delay_seconds = max(0.0, float(delay_seconds))
        self.refresh_web = refresh_web
        self.cache = cache
        self.session = session or requests.Session()

    def _fetch(self, url: str, fetcher: Optional[FetchFn]) -> FetchResult:
        if not self.refresh_web and self.cache is not None:
            cached = self.cache.get(url)
            if cached is not None:
                return cached
        result = fetcher(url) if fetcher is not None else _fetch_with_requests(url, self.session)
        if self.cache is not None and not result.error:
            self.cache.set(result)
        return result

    def crawl(self, start_url: str, evidence_pages_dir: Path, fetcher: Optional[FetchFn] = None) -> CrawlResult:
        evidence_pages_dir.mkdir(parents=True, exist_ok=True)

        parsed_start = urlparse(start_url)
        base_netloc = normalize_netloc(parsed_start.netloc)
        if not base_netloc:
            raise ValueError(f"Invalid start URL (missing netloc): {start_url}")

        import heapq

        heap: List[tuple[int, int, str]] = []
        heapq.heappush(heap, (-100, 0, start_url))
        visited: set[str] = set()
        discovered: set[str] = {start_url}

        root_url = f"{parsed_start.scheme}://{parsed_start.netloc}/"
        for seed_path in SEED_PATHS:
            seed_url = urljoin(root_url, seed_path)
            if not same_domain(seed_url, base_netloc):
                continue
            if seed_url in discovered:
                continue
            discovered.add(seed_url)
            heapq.heappush(heap, (-SEED_PRIORITY, 1, seed_url))

        pages: List[CrawlPage] = []
        errors: List[str] = []
        cache_hits = 0
        live_fetches = 0
        found_pricing_pages = 0

        while heap and len(pages) < self.max_pages:
            neg_priority, depth, url = heapq.heappop(heap)
            if url in visited:
                continue
            if not same_domain(url, base_netloc):
                continue
            visited.add(url)

            result = self._fetch(url, fetcher)
            if result.from_cache:
                cache_hits += 1
            else:
                live_fetches += 1
            if result.error:
                errors.append(f"{url}: {result.error}")
                continue
            if result.status_code >= 400:
                errors.append(f"{url}: http_{result.status_code}")
                continue

            links: List[ExtractedLink] = []
            forms: List[str] = []
            mailto_emails: List[str] = []
            pdf_links: List[str] = []
            visible_text = ""

            is_html = result.content_type.startswith("text/html") or result.final_url.lower().endswith(".html")
            if is_html and result.text:
                (
                    extracted_links,
                    forms,
                    mailto_emails,
                    pdf_links,
                    visible_text,
                ) = extract_links_forms_emails_pdfs(result.text, base_url=result.final_url)

                # Enqueue same-domain HTML pages only.
                for link in extracted_links:
                    link_url = link.url
                    lowered = link_url.lower()
                    if lowered.startswith("mailto:") or lowered.endswith(".pdf"):
                        continue
                    if link_url in visited or link_url in discovered:
                        continue
                    if not same_domain(link_url, base_netloc):
                        continue
                    discovered.add(link_url)
                    adjusted_priority = link.priority - depth * 5
                    heapq.heappush(heap, (-adjusted_priority, depth + 1, link_url))
                links = [l for l in extracted_links if same_domain(l.url, base_netloc)]
                pdf_links = [p for p in pdf_links if same_domain(p, base_netloc)]

                # Snapshot HTML for evidence.
                page_index = len(pages) + 1
                path_part = urlparse(result.final_url).path or "/"
                slug = ascii_slug(path_part)[:80]
                snapshot_path = evidence_pages_dir / f"{page_index:03d}_{slug}.html"
                snapshot_path.write_text(result.text, encoding="utf-8")

            is_strong_pricing = _is_likely_pricing_page(result.final_url, visible_text or result.text)

            page = CrawlPage(
                index=len(pages) + 1,
                url=url,
                final_url=result.final_url,
                status_code=result.status_code,
                content_type=result.content_type,
                text=result.text,
                visible_text=visible_text,
                links=links,
                forms=forms,
                mailto_emails=mailto_emails,
                pdf_links=pdf_links,
                is_pricing_candidate=is_strong_pricing,
                is_contact_candidate=is_contact_url(result.final_url),
            )
            pages.append(page)

            if self.delay_seconds and not result.from_cache:
                time.sleep(self.delay_seconds)

            # Early stop check
            if is_strong_pricing:
                found_pricing_pages += 1
                if found_pricing_pages >= 1:
                    logger.info("Found likely pricing page at %s, stopping crawl early.", result.final_url)
                    break

        visited_urls = sorted(visited)
        if len(pages) >= self.max_pages:
            logger.info("Crawl capped at max_pages=%s for %s", self.max_pages, start_url)
        return CrawlResult(
            start_url=start_url,
            base_netloc=base_netloc,
            pages=pages,
            visited_urls=visited_urls,
            errors=errors,
            cache_hits=cache_hits,
            live_fetches=live_fetches,
        )
