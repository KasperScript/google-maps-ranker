"""HTML extraction helpers for the outreach stage."""
from __future__ import annotations

import html
import re
import unicodedata
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

PRIORITY_KEYWORDS: Sequence[tuple[str, int]] = (
    # Pricing-first signals (ranked, deterministic).
    ("cennik", 100),
    ("cennik-uslug", 88),
    ("cennik-uslug-stomatologicznych", 78),
    ("cennik-ortodoncja", 76),
    ("cennikortodontyczny", 62),
    ("cennik usług", 92),
    ("cennik ortodoncji", 90),
    ("cennik ortodontyczny", 88),
    ("ceny", 82),
    ("cena", 76),
    ("koszt", 74),
    # Orthodontic pricing hints (exclude Invisalign by request).
    ("ortodoncja", 56),
    ("aparat-staly", 55),
    ("aparat stały", 55),
    ("aparat metalowy", 46),
    ("aparat estetyczny", 42),
    ("wizyta kontrolna", 38),
    ("zalozenie aparatu", 34),
    ("założenie aparatu", 34),
    ("konsultacja ortodontyczna", 32),
    # English pricing variants seen on Polish sites.
    ("price list", 70),
    ("price-list", 70),
    ("pricing", 62),
    ("prices", 58),
    # Contact remains useful but is lower priority than pricing.
    ("kontakt", 35),
    ("contact", 35),
)

PRICING_KEYWORDS = {
    "cennik",
    "pricing",
    "oferta",
    "uslugi",
    "usługi",
    "koszt",
    "price",
    "cena",
    "ceny",
}

CONTACT_KEYWORDS = {
    "kontakt",
    "contact",
    "kontaktowy",
    "contact-us",
    "contactus",
}

NEGATIVE_URL_TOKEN_PENALTIES: Sequence[tuple[str, int]] = (
    ("wp-content", 98),
    ("wp-json", 98),
    ("feed", 96),
    ("blog", 95),
    ("aktualnosci", 92),
    ("news", 90),
    ("tag", 90),
    ("kategoria", 88),
    ("category", 88),
    ("author", 85),
    ("search", 80),
    ("page", 75),
    ("sitemap", 70),
    ("galeria", 65),
    ("portfolio", 65),
    ("regulamin", 60),
    ("polityka-prywatnosci", 60),
    ("cookies", 55),
    ("rodo", 55),
)

ORTHO_HINTS = {
    "ortod",
    "orthodont",
    "aparat",
    "braces",
    "retenc",
}

EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,}\b")


def normalize_netloc(netloc: str) -> str:
    netloc = (netloc or "").strip().lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    # Drop default ports.
    if netloc.endswith(":80"):
        netloc = netloc[:-3]
    if netloc.endswith(":443"):
        netloc = netloc[:-4]
    return netloc


def same_domain(url: str, base_netloc: str) -> bool:
    parsed = urlparse(url)
    netloc = normalize_netloc(parsed.netloc)
    base = normalize_netloc(base_netloc)
    if not netloc or not base:
        return False
    if netloc == base:
        return True
    return netloc.endswith(f".{base}")


def _strip_fragment(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.fragment:
        return url
    parsed = parsed._replace(fragment="")
    return urlunparse(parsed)


def absolutize_url(base_url: str, href: str) -> Optional[str]:
    href = (href or "").strip()
    if not href:
        return None
    lowered = href.lower()
    if lowered.startswith("javascript:"):
        return None
    if lowered.startswith("tel:"):
        return None
    # mailto links are handled separately.
    if lowered.startswith("mailto:"):
        return href
    joined = urljoin(base_url, href)
    return _strip_fragment(joined)


def _keyword_score(text: str) -> int:
    score = 0
    lowered = (text or "").lower()
    for keyword, weight in PRIORITY_KEYWORDS:
        if keyword in lowered:
            score += weight
    return score


def _path_segments(url: str) -> list[str]:
    path = urlparse(url).path or "/"
    return [seg for seg in path.lower().split("/") if seg]


def _has_segment(url: str, token: str) -> bool:
    token = (token or "").strip().lower()
    if not token:
        return False
    return token in _path_segments(url)


def link_priority(url: str, anchor_text: str = "") -> int:
    url_score = _keyword_score(url)
    anchor_score = _keyword_score(anchor_text)
    score = url_score + anchor_score

    lowered_url = (url or "").lower()
    has_cennik = "cennik" in lowered_url or _has_segment(url, "cennik")

    # Strong priors for canonical pricing segments and co-occurrence hints.
    if _has_segment(url, "cennik"):
        score += 40
    if has_cennik and ("ortodoncja" in lowered_url or _has_segment(url, "ortodoncja")):
        score += 25
    if has_cennik and ("aparat-staly" in lowered_url or "aparat stały" in anchor_text.lower()):
        score += 15

    # Apply hard penalties for crawl-budget traps unless the URL is clearly pricing.
    if not has_cennik:
        for token, penalty in NEGATIVE_URL_TOKEN_PENALTIES:
            if token in lowered_url:
                score -= penalty

    # Slight preference for shorter paths.
    parsed = urlparse(url)
    path_len = len(parsed.path or "")
    score -= min(path_len // 20, 10)
    return score


def is_pricing_url(url: str) -> bool:
    lowered = (url or "").lower()
    return any(k in lowered for k in PRICING_KEYWORDS)


def is_contact_url(url: str) -> bool:
    lowered = (url or "").lower()
    return any(k in lowered for k in CONTACT_KEYWORDS)


def hints_braces_or_ortho(text: str) -> bool:
    lowered = (text or "").lower()
    return any(hint in lowered for hint in ORTHO_HINTS)


def ascii_slug(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.lower()
    ascii_text = re.sub(r"[^a-z0-9]+", "-", ascii_text)
    ascii_text = ascii_text.strip("-")
    return ascii_text or "clinic"


@dataclass(frozen=True)
class ExtractedLink:
    url: str
    anchor_text: str
    priority: int


class _LinkFormParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links: List[Tuple[str, str]] = []
        self.forms: List[str] = []
        self.mailto_emails: List[str] = []
        self._in_script_or_style = 0
        self._current_href: Optional[str] = None
        self._current_anchor_parts: List[str] = []
        self.visible_text_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        tag = tag.lower()
        attrs_dict = {k.lower(): (v or "") for k, v in attrs}
        if tag in {"script", "style", "noscript"}:
            self._in_script_or_style += 1
            return
        if tag == "a":
            href = attrs_dict.get("href")
            if href:
                abs_href = absolutize_url(self.base_url, href)
                if abs_href:
                    self._current_href = abs_href
                    self._current_anchor_parts = []
            return
        if tag == "form":
            action = attrs_dict.get("action")
            if action:
                abs_action = absolutize_url(self.base_url, action)
                if abs_action:
                    self.forms.append(abs_action)
            else:
                # If no action, the current URL is a reasonable candidate.
                self.forms.append(self.base_url)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript"} and self._in_script_or_style:
            self._in_script_or_style -= 1
            return
        if tag == "a" and self._current_href:
            anchor_text = " ".join(p for p in self._current_anchor_parts if p).strip()
            self.links.append((self._current_href, anchor_text))
            self._current_href = None
            self._current_anchor_parts = []

    def handle_data(self, data: str) -> None:
        if not data:
            return
        if self._in_script_or_style:
            return
        if self._current_href is not None:
            self._current_anchor_parts.append(data.strip())
            return
        cleaned = data.strip()
        if cleaned:
            self.visible_text_parts.append(cleaned)


def extract_links_forms_emails_pdfs(html_text: str, base_url: str) -> tuple[List[ExtractedLink], List[str], List[str], List[str], str]:
    parser = _LinkFormParser(base_url)
    try:
        parser.feed(html_text)
    except Exception:
        # Fall back to regex extraction below if parsing fails.
        pass

    links_out: List[ExtractedLink] = []
    seen_links: set[str] = set()
    mailto_emails: set[str] = set()
    pdf_links: set[str] = set()

    for href, anchor_text in parser.links:
        lowered = href.lower()
        if lowered.startswith("mailto:"):
            email = lowered.replace("mailto:", "", 1).split("?", 1)[0].strip()
            if email:
                mailto_emails.add(email)
            continue
        if href in seen_links:
            continue
        seen_links.add(href)
        if lowered.endswith(".pdf"):
            pdf_links.add(href)
        links_out.append(ExtractedLink(url=href, anchor_text=anchor_text, priority=link_priority(href, anchor_text)))

    forms = sorted({f for f in parser.forms if f})

    # Some sites embed mailto in raw HTML without a proper <a> tag.
    for mailto in re.findall(r"mailto:([^\"'\s>]+)", html_text, flags=re.IGNORECASE):
        email = mailto.split("?", 1)[0].strip().lower()
        if email:
            mailto_emails.add(email)

    visible_text = " ".join(parser.visible_text_parts)
    visible_text = html.unescape(visible_text)
    visible_text = re.sub(r"\s+", " ", visible_text).strip()

    return (
        sorted(links_out, key=lambda l: (-l.priority, l.url)),
        forms,
        sorted(mailto_emails),
        sorted(pdf_links),
        visible_text,
    )


def extract_emails(texts: Iterable[str], extra_emails: Optional[Iterable[str]] = None) -> List[str]:
    emails: set[str] = set()
    for text in texts:
        if not text:
            continue
        for match in EMAIL_RE.findall(text):
            emails.add(match.lower())
    if extra_emails:
        for email in extra_emails:
            email = (email or "").strip().lower()
            if email and "@" in email:
                emails.add(email)
    return sorted(emails)


def extract_visible_text(html_text: str) -> str:
    _, _, _, _, visible_text = extract_links_forms_emails_pdfs(html_text, base_url="http://example.invalid/")
    return visible_text
