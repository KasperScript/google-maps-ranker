"""Outreach stage pipeline.

This stage is deliberately conservative:
- It only crawls a clinic's own domain.
- It never sends emails or submits forms.
- It produces a manual-review queue plus evidence artifacts.
"""
from __future__ import annotations

import csv
import json
import logging
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence
from urllib.parse import urlparse

import requests

from src.gemini_client import BaseGeminiClient, GeminiCallResult, GeminiClient, hash_text
from src.reporting import atomic_write_text, atomic_writer, ensure_dir

from .crawler import CrawlResult, DomainLimitedCrawler, PageCache
from .extractors import (
    ExtractedLink,
    ascii_slug,
    extract_emails,
    extract_visible_text,
    hints_braces_or_ortho,
    is_contact_url,
    is_pricing_url,
    normalize_netloc,
)
from .playwright_assist import AssistResult, run_playwright_assist

logger = logging.getLogger(__name__)

DEFAULT_OUTREACH_DIR = Path("out/outreach")
DEFAULT_MAX_PAGES = 30
PRICING_TEXT_MIN_CHARS = 500
SKIPPED_MISSING_WEBSITE_STATUS = "skipped_missing_website"

EMAIL_TEMPLATE_PATH = Path("prompts/email_template_no_pricing_pl.txt")
PROMPT_PRICE_CALC_PATH = Path("prompts/gemini_price_calc_v3.txt")
PROMPT_OUTREACH_PATH = Path("prompts/gemini_outreach_message_v2.txt")

WEBSITE_COLUMNS: Sequence[str] = (
    "website",
    "website_url",
    "website_uri",
    "websiteUri",
    "websiteUrl",
    "site",
    "url",
)

DO_NOT_CONTACT_NAMES = {
    "declinic",
    "ewelina-iwanczyk",
}

GMAIL_SKIP_EXACT_NAMES = {
    "DeClinic",
    "Ewelina Iwańczyk",
}

DEFAULT_GMAIL_LABEL = "OrthoRanker"

DEFAULT_MISSING_PRICES = [
    "orthodontic consultation",
    "treatment plan/analysis",
    "3D scan / digital models",
    "RTG OPG",
    "RTG cephalometric",
    "metal fixed braces 1 arch",
    "control 1 arch",
    "control 2 arches",
    "debond 1 arch",
    "fixed retainer 1 arch",
    "removable retainer 1 arch",
    "retention control visit",
]

MUST_PRESERVE_PHRASE_1 = (
    "rozważam leczenie aparatem stałym (metalowym lub ICONIX). Opis na stronie jest bardzo zachęcający i mam poczucie, "
    "że podchodzą Państwo do leczenia profesjonalnie, dlatego chciałbym poprosić o orientacyjną wycenę i komplet informacji "
    "o kosztach, żebym mógł spokojnie zaplanować budżet."
)
MUST_PRESERVE_PHRASE_2 = (
    "Na koniec, czy są jeszcze jakieś typowe koszty dodatkowe, które warto uwzględnić w budżecie (np. awarie/naprawy elementów, dodatkowe badania)?"
)

MISSING_PRICE_KEYWORDS = {
    "orthodontic consultation": [r"konsultacj", r"ortodont"],
    "treatment plan/analysis": [r"plan leczenia", r"analiz", r"plan terapii"],
    "3D scan / digital models": [r"skan", r"3d", r"model", r"skaner"],
    "RTG OPG": [r"opg", r"pantom", r"rtg opg"],
    "RTG cephalometric": [r"cefalo", r"cefal", r"rtg cefal"],
    "metal fixed braces 1 arch": [r"aparat", r"metal", r"stał"],
    "control 1 arch": [r"wizyta kontrol", r"kontrola", r"1 łuk", r"jeden łuk"],
    "control 2 arches": [r"2 łuk", r"dwa łuki", r"dwa łuk"],
    "debond 1 arch": [r"zdjęcie aparatu", r"debon", r"usunięcie aparatu"],
    "fixed retainer 1 arch": [r"retainer stały", r"retencja stała", r"drucik"],
    "removable retainer 1 arch": [r"retainer ruchomy", r"aparat retencyjny", r"retencja ruchoma"],
    "retention control visit": [r"wizyta retencyjna", r"kontrola retencyjna"],
}

ORTHO_HINTS_RE = re.compile(r"(ortod|orthodont|aparat|braces)", re.IGNORECASE)

PRICE_CATEGORY_PATTERNS: Dict[str, List[str]] = {
    "bonding": [r"aparat", r"iconix", r"metal", r"1\s*łuk", r"2\s*łuk", r"dwa\s*łuk"],
    "controls": [r"wizyt\s+kontrol", r"kontroln", r"w\s+trakcie\s+leczenia"],
    "diagnostics": [r"dokumentac", r"planowania", r"plan\s+leczenia", r"analiz"],
    "debond": [r"zdjęcia\s+aparatu", r"demontaż", r"oczyszczeniem\s+z\s+kleju", r"polerowaniem"],
    "retention_controls": [r"wizyt\s+kontrolnych\s+w\s+retencji", r"kontrola\s+retenc"],
    "retention": [r"retencj", r"retainer", r"płytka", r"szyna"],
    "hygiene": [r"higienizac", r"higiena"],
}

# Categories that must ALWAYS be asked about if missing, even if kept_items is empty.
# These are critical costs that significantly impact total treatment price.
CRITICAL_CATEGORIES = {"bonding", "controls", "debond", "retention", "retention_controls"}


MISSING_KEY_TO_CATEGORY = {
    # Human-readable keys (original)
    "orthodontic consultation": "diagnostics",
    "treatment plan/analysis": "diagnostics",
    "3D scan / digital models": "diagnostics",
    "RTG OPG": "diagnostics",
    "RTG cephalometric": "diagnostics",
    "OPG+ceph package": "diagnostics",
    "metal fixed braces 1 arch": "bonding",
    "self-ligating / Damon 1 arch": "bonding",
    "control 1 arch": "controls",
    "control 2 arches": "controls",
    "debond 1 arch": "debond",
    "fixed retainer 1 arch": "retention",
    "removable retainer 1 arch": "retention",
    "package debond+retention 2 arches": "retention",
    "retention control visit": "retention_controls",
    # Gemini's actual key formats (underscore-separated, different naming)
    "orthodontic_consultation": "diagnostics",
    "treatment_plan": "diagnostics",
    "treatment_plan_analysis": "diagnostics",
    "3d_scan": "diagnostics",
    "digital_models": "diagnostics",
    "rtg_opg": "diagnostics",
    "rtg_cephalometric": "diagnostics",
    "opg_ceph_package": "diagnostics",
    "fixed_braces_bonding": "bonding",
    "metal_fixed_braces_1_arch": "bonding",
    "self_ligating_damon_1_arch": "bonding",
    "control_visit_1_arch": "controls",
    "control_visit_2_arches": "controls",
    "control_1_arch": "controls",
    "control_2_arches": "controls",
    "debond_1_arch": "debond",
    "debonding": "debond",
    "fixed_retainer_1_arch": "retention",
    "removable_retainer_1_arch": "retention",
    "retention": "retention",
    "package_debond_retention_2_arches": "retention",
    "retention_control_visit": "retention_controls",
}


@dataclass(frozen=True)
class OutreachEvidenceDirs:
    clinic_root: Path
    pages_dir: Path
    pdf_dir: Path
    screenshots_dir: Path
    gemini_dir: Path
    extracted_pricing_path: Path


@dataclass(frozen=True)
class OutreachConfig:
    input_csv_path: Path
    out_dir: Path = DEFAULT_OUTREACH_DIR
    top_n: int = 30
    max_pages: int = DEFAULT_MAX_PAGES
    refresh_web: bool = False
    refresh_places: bool = False
    playwright_assist: bool = False


@dataclass(frozen=True)
class OutreachRunResult:
    run_id: str
    run_dir: Path
    results_path: Path
    queue_path: Path
    summary_path: Path
    qa_report_path: Path
    gmail_report_path: Optional[Path]
    evidence_root: Path
    processed_count: int


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_float(val: Any) -> float:
    try:
        if val is None or val == "":
            return float("-inf")
        return float(val)
    except (TypeError, ValueError):
        return float("-inf")


def _safe_int(val: Any) -> int:
    try:
        if val is None or val == "":
            return -1
        return int(val)
    except (TypeError, ValueError):
        return -1


def _sort_key(row: Dict[str, Any]) -> tuple[float, float, int, str]:
    quality = _safe_float(row.get("quality"))
    rating = _safe_float(row.get("rating"))
    user_ratings = _safe_int(row.get("user_rating_count"))
    name = str(row.get("name") or "")
    # Descending numeric metrics.
    return (quality, rating, user_ratings, name)


def _read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def _extract_rows_from_json_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("results", "items", "data", "clinics"):
            val = payload.get(key)
            if isinstance(val, list):
                return [dict(item) for item in val if isinstance(item, dict)]
        if "name" in payload:
            return [dict(payload)]
    raise ValueError(
        "Unsupported JSON outreach input shape; expected a list or a dict with a list under results/items/data/clinics."
    )


def _read_json_rows(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return _extract_rows_from_json_payload(payload)


def _read_input_rows(path: Path) -> tuple[List[Dict[str, Any]], bool]:
    """Read outreach input rows and indicate whether to preserve ranking order."""
    suffix = path.suffix.lower()
    if suffix == ".json":
        return _read_json_rows(path), True
    return _read_csv_rows(path), False


def _filter_rows_with_name(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        out.append(row)
    return out


def find_latest_merged_csv(base_dir: Path = Path("out")) -> Optional[Path]:
    if not base_dir.exists():
        return None
    candidates = list(base_dir.rglob("radius_scan_merged_results.csv"))
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _normalize_name_for_blocklist(name: str) -> str:
    return ascii_slug(name or "")


def _is_do_not_contact(name: str) -> bool:
    slug = _normalize_name_for_blocklist(name)
    return slug in DO_NOT_CONTACT_NAMES


def _looks_like_google_maps(url: str) -> bool:
    netloc = normalize_netloc(urlparse(url).netloc)
    return netloc.endswith("google.com") or netloc.endswith("googleusercontent.com")


def _pick_website(row: Dict[str, Any]) -> Optional[str]:
    for col in WEBSITE_COLUMNS:
        val = (row.get(col) or "").strip()
        if not val:
            continue
        if _looks_like_google_maps(val):
            continue
        parsed = urlparse(val)
        if parsed.scheme and parsed.netloc:
            return val
        # If it looks like a bare domain, assume https.
        if parsed.netloc and not parsed.scheme:
            return f"https://{parsed.netloc}"
        if parsed.path and "." in parsed.path and " " not in parsed.path:
            return f"https://{parsed.path}"
    return None


def _clinic_slug(row: Dict[str, Any], rank: int) -> str:
    name = str(row.get("name") or "clinic")
    place_id = str(row.get("place_id") or "")
    base = f"{name}-{place_id}" if place_id else f"{name}-{rank}"
    slug = ascii_slug(base)
    return slug[:120]


def _ensure_evidence_dirs(evidence_root: Path, clinic_slug: str) -> OutreachEvidenceDirs:
    clinic_root = evidence_root / clinic_slug
    pages_dir = clinic_root / "pages"
    pdf_dir = clinic_root / "pdf"
    screenshots_dir = clinic_root / "screenshots"
    gemini_dir = clinic_root / "gemini"
    for d in (clinic_root, pages_dir, pdf_dir, screenshots_dir, gemini_dir):
        d.mkdir(parents=True, exist_ok=True)
    return OutreachEvidenceDirs(
        clinic_root=clinic_root,
        pages_dir=pages_dir,
        pdf_dir=pdf_dir,
        screenshots_dir=screenshots_dir,
        gemini_dir=gemini_dir,
        extracted_pricing_path=clinic_root / "extracted_pricing.txt",
    )


def _load_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Required prompt/template file missing: {path}")
    return path.read_text(encoding="utf-8")


def _replace_tokens(template: str, tokens: Dict[str, str]) -> str:
    out = template
    for key, val in tokens.items():
        out = out.replace(f"{{{key}}}", val)
    return out


def _validate_price_calc(data: Dict[str, Any]) -> None:
    required_top = {"clinic_name", "currency", "evidence_level", "extracted_prices", "variants", "notes"}
    missing_top = required_top - data.keys()
    if missing_top:
        raise ValueError(f"missing_top_keys: {sorted(missing_top)}")
    variants = data.get("variants")
    if not isinstance(variants, dict):
        raise ValueError("variants_not_dict")
    for key in ("A", "B", "C"):
        if key not in variants:
            raise ValueError(f"missing_variant_{key}")
        if not isinstance(variants[key], dict):
            raise ValueError(f"variant_{key}_not_dict")


def _validate_outreach_message(data: Dict[str, Any]) -> None:
    required_top = {
        "clinic_name",
        "subject",
        "body",
        "questions_missing_prices",
        "template_preservation_check",
    }
    missing_top = required_top - data.keys()
    if missing_top:
        raise ValueError(f"missing_top_keys: {sorted(missing_top)}")


def _write_json(path: Path, payload: Any) -> None:
    with atomic_writer(str(path), mode="w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    with atomic_writer(str(path), mode="w", encoding="utf-8", newline="") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _dedup_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen_place_ids: set[str] = set()
    seen_names: set[str] = set()
    out: List[Dict[str, Any]] = []
    for row in rows:
        place_id = str(row.get("place_id") or "").strip()
        name_key = _normalize_name_for_blocklist(str(row.get("name") or ""))
        if place_id and place_id in seen_place_ids:
            continue
        if name_key and name_key in seen_names:
            continue
        if place_id:
            seen_place_ids.add(place_id)
        if name_key:
            seen_names.add(name_key)
        out.append(row)
    return out


def _is_pricing_page(page) -> bool:
    if getattr(page, "is_pricing_candidate", False):
        return True
    visible_text = getattr(page, "visible_text", "")
    if not visible_text:
        return False
    lowered = visible_text.lower()
    return any(k in lowered for k in ("cennik", "cena", "ceny", "pricing", "price", "koszt"))


def _is_contact_page(page) -> bool:
    if getattr(page, "is_contact_candidate", False):
        return True
    url = getattr(page, "final_url", "")
    return is_contact_url(url)


def _pricing_text_from_pages(pricing_pages: List[Any]) -> str:
    parts: List[str] = []
    for page in pricing_pages[:5]:
        text = page.visible_text or extract_visible_text(page.text)
        if not text:
            continue
        header = f"URL: {page.final_url}"
        parts.append(header)
        parts.append(text)
    combined = "\n\n".join(parts).strip()
    return combined


def _collect_pdf_urls(pages: List[Any]) -> List[str]:
    pdfs: set[str] = set()
    for page in pages:
        for pdf_url in getattr(page, "pdf_links", []) or []:
            pdfs.add(pdf_url)
        for link in getattr(page, "links", []) or []:
            url = link.url if isinstance(link, ExtractedLink) else str(link)
            if url.lower().endswith(".pdf"):
                pdfs.add(url)
    return sorted(pdfs)


def _download_pdf(url: str, dest_dir: Path, session: Optional[requests.Session] = None) -> Optional[str]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    sess = session or requests.Session()
    try:
        resp = sess.get(url, timeout=20, stream=True)
    except requests.RequestException as exc:
        logger.warning("PDF download failed for %s: %s", url, exc)
        return None
    if resp.status_code >= 400:
        logger.warning("PDF download HTTP %s for %s", resp.status_code, url)
        return None
    file_name = ascii_slug(Path(urlparse(url).path).stem or "pricing")[:80] + ".pdf"
    path = dest_dir / file_name
    total = 0
    try:
        with path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=32_768):
                if not chunk:
                    continue
                f.write(chunk)
                total += len(chunk)
                if total >= 10_000_000:
                    break
    finally:
        resp.close()
    return str(path)


def _collect_missing_prices(price_calc_data: Optional[Dict[str, Any]]) -> List[str]:
    if not price_calc_data:
        return list(DEFAULT_MISSING_PRICES)
    variants = price_calc_data.get("variants") or {}
    missing: set[str] = set()
    for key in ("A", "B", "C"):
        entry = variants.get(key) or {}
        for item in entry.get("missing") or []:
            missing.add(str(item))
    if not missing:
        return []
    return sorted(missing)


def _is_bullet_line(line: str) -> bool:
    stripped = line.lstrip()
    return stripped.startswith(("-", "•", "*"))


def _line_matches_keywords(line: str, keywords: List[str]) -> bool:
    if not keywords:
        return False
    for pattern in keywords:
        try:
            if re.search(pattern, line, flags=re.IGNORECASE):
                return True
        except re.error:
            if pattern.lower() in line.lower():
                return True
    return False


def _extract_subject_and_body(template_text: str) -> tuple[str, str]:
    subject = ""
    body_lines: List[str] = []
    in_body = False
    for raw_line in template_text.splitlines():
        line = raw_line.rstrip()
        lowered = line.lower().strip()
        if lowered.startswith("subject:"):
            subject = line.split(":", 1)[1].strip()
            continue
        if lowered.startswith("body:"):
            in_body = True
            body_lines.append(line.split(":", 1)[1].lstrip())
            continue
        if in_body:
            body_lines.append(raw_line)
        else:
            body_lines.append(raw_line)

    body = "\n".join(body_lines).strip()
    # Remove wrapping quotes if present.
    if subject.startswith('"') and subject.endswith('"') and len(subject) > 1:
        subject = subject[1:-1]
    if body.startswith('"') and body.endswith('"') and len(body) > 1:
        body = body[1:-1]
    return subject.strip(), body.strip()


def _classify_question_line(line: str) -> Optional[str]:
    text = line.strip()
    if not text:
        return None
    # Order matters for more specific categories.
    for category in ("retention_controls", "retention", "debond", "controls", "diagnostics", "bonding", "hygiene"):
        patterns = PRICE_CATEGORY_PATTERNS.get(category, [])
        if _line_matches_keywords(text, patterns):
            return category
    return None


def _known_categories_from_text(raw_text: str) -> set[str]:
    if not raw_text:
        return set()
    known: set[str] = set()
    for line in raw_text.splitlines():
        if not line.strip():
            continue
        has_price_hint = bool(re.search(r"(\d{2,}|zł|pln)", line, flags=re.IGNORECASE))
        if not has_price_hint:
            continue
        for category, patterns in PRICE_CATEGORY_PATTERNS.items():
            if _line_matches_keywords(line, patterns):
                known.add(category)
    return known


def _categories_from_price_calc(price_calc_data: Optional[Dict[str, Any]]) -> tuple[set[str], set[str]]:
    if not price_calc_data:
        return set(), set()
    missing_items = _collect_missing_prices(price_calc_data)
    missing_categories: set[str] = set()
    supported_categories: set[str] = set()
    for key, category in MISSING_KEY_TO_CATEGORY.items():
        supported_categories.add(category)
        if any(str(item).strip().lower() == key.lower() for item in missing_items):
            missing_categories.add(category)
    known_categories = supported_categories - missing_categories
    return missing_categories, known_categories


def _render_template_outreach(
    *,
    clinic_name: str,
    clinic_website: str,
    template_text: str,
    pricing_status: str,
    missing_prices: List[str],
    price_calc_data: Optional[Dict[str, Any]],
    raw_pricing_text: str,
) -> Dict[str, Any]:
    subject, body = _extract_subject_and_body(template_text or "")
    if not subject:
        subject = f"Prośba o orientacyjną wycenę – {clinic_name}".strip(" –")

    body = body.replace("{CLINIC_NAME}", clinic_name).replace("{CLINIC_WEBSITE}", clinic_website or "")

    missing_by_calc, known_by_calc = _categories_from_price_calc(price_calc_data)
    known_by_text = _known_categories_from_text(raw_pricing_text)

    question_pattern = re.compile(r"^(\s*)(\d+)([.)])\s+")
    question_lines: List[Dict[str, Any]] = []
    output_lines: List[str] = []
    for line in body.splitlines():
        match = question_pattern.match(line)
        if match:
            category = _classify_question_line(line)
            question_lines.append(
                {
                    "line": line,
                    "category": category,
                    "prefix": match.group(1),
                    "suffix": match.group(3),
                    "rest": line[match.end() :],
                }
            )
            output_lines.append(line)
        else:
            output_lines.append(line)

    categories_from_questions = {q["category"] for q in question_lines if q["category"]}
    known_categories = set(known_by_text) | set(known_by_calc)
    missing_categories = set(categories_from_questions) - known_categories
    missing_categories |= set(missing_by_calc)

    removed_items: List[Dict[str, Any]] = []
    kept_items: List[Dict[str, Any]] = []

    new_lines: List[str] = []
    new_index = 1
    for line in output_lines:
        match = question_pattern.match(line)
        if not match:
            new_lines.append(line)
            continue
        category = _classify_question_line(line)
        if category and category not in missing_categories:
            removed_items.append({"line": line, "category": category})
            continue
        kept_items.append({"line": line, "category": category})
        prefix, suffix, rest = match.group(1), match.group(3), line[match.end() :]
        new_lines.append(f"{prefix}{new_index}{suffix} {rest}")
        new_index += 1

    final_body = "\n".join(new_lines).strip()

    preserved_phrase_1 = MUST_PRESERVE_PHRASE_1 in final_body
    preserved_phrase_2 = MUST_PRESERVE_PHRASE_2 in final_body

    # Check if any CRITICAL categories are still missing - if so, we must send an email
    critical_missing = missing_categories & CRITICAL_CATEGORIES
    
    if not kept_items and not critical_missing:
        return {
            "status": "all_prices_found",
            "subject": subject,
            "body": final_body,
            "missing_categories": sorted(missing_categories),
            "known_categories": sorted(known_categories),
            "missing_by_calc": sorted(missing_by_calc),
            "known_by_calc": sorted(known_by_calc),
            "known_by_text": sorted(known_by_text),
            "removed_items": removed_items,
            "kept_items": kept_items,
            "pricing_status": pricing_status,
            "template_preservation_check": {
                "preserved_phrase_1": preserved_phrase_1,
                "preserved_phrase_2": preserved_phrase_2,
            },
        }


    return {
        "status": "template",
        "clinic_name": clinic_name,
        "subject": subject,
        "body": final_body,
        "questions_missing_prices": missing_prices,
        "missing_categories": sorted(missing_categories),
        "known_categories": sorted(known_categories),
        "missing_by_calc": sorted(missing_by_calc),
        "known_by_calc": sorted(known_by_calc),
        "known_by_text": sorted(known_by_text),
        "removed_items": removed_items,
        "kept_items": kept_items,
        "pricing_status": pricing_status,
        "template_preservation_check": {
            "preserved_phrase_1": preserved_phrase_1,
            "preserved_phrase_2": preserved_phrase_2,
        },
    }


def _is_bullet_line(line: str) -> bool:
    stripped = line.lstrip()
    return stripped.startswith(("-", "•", "*"))


def _line_matches_keywords(line: str, keywords: List[str]) -> bool:
    if not keywords:
        return False
    for pattern in keywords:
        try:
            if re.search(pattern, line, flags=re.IGNORECASE):
                return True
        except re.error:
            if pattern.lower() in line.lower():
                return True
    return False


def _extract_subject_and_body(template_text: str) -> tuple[str, str]:
    subject = ""
    body_lines: List[str] = []
    for raw_line in template_text.splitlines():
        line = raw_line.rstrip()
        lowered = line.lower().strip()
        if not subject and (lowered.startswith("subject:") or lowered.startswith("temat:")):
            subject = line.split(":", 1)[1].strip()
            continue
        body_lines.append(raw_line)
    body = "\n".join(body_lines).strip()
    return subject, body


def _fallback_outreach_message(
    *,
    clinic_name: str,
    clinic_website: str,
    pricing_status: str,
    missing_prices: List[str],
    template_text: str,
) -> Dict[str, Any]:
    subject, body = _extract_subject_and_body(template_text or "")
    if not subject:
        subject = f"Prośba o orientacyjną wycenę – {clinic_name}".strip(" –")

    body = body.replace("{CLINIC_NAME}", clinic_name).replace("{CLINIC_WEBSITE}", clinic_website or "")

    missing_set = {str(item).strip().lower() for item in missing_prices if str(item).strip()}
    missing_keys = [key for key in MISSING_PRICE_KEYWORDS if key.lower() in missing_set]
    known_keys = [key for key in MISSING_PRICE_KEYWORDS if key.lower() not in missing_set]

    kept_lines: List[str] = []
    for line in body.splitlines():
        if MUST_PRESERVE_PHRASE_1 in line or MUST_PRESERVE_PHRASE_2 in line:
            kept_lines.append(line)
            continue
        if _is_bullet_line(line):
            matches_missing = any(_line_matches_keywords(line, MISSING_PRICE_KEYWORDS.get(k, [])) for k in missing_keys)
            matches_known = any(_line_matches_keywords(line, MISSING_PRICE_KEYWORDS.get(k, [])) for k in known_keys)
            if matches_missing:
                kept_lines.append(line)
                continue
            if matches_known:
                continue
        kept_lines.append(line)

    body = "\n".join(kept_lines).strip()
    preserved_phrase_1 = MUST_PRESERVE_PHRASE_1 in body
    preserved_phrase_2 = MUST_PRESERVE_PHRASE_2 in body

    return {
        "clinic_name": clinic_name,
        "subject": subject,
        "body": body,
        "pricing_status": pricing_status,
        "questions_missing_prices": missing_prices,
        "template_preservation_check": {
            "preserved_phrase_1": preserved_phrase_1,
            "preserved_phrase_2": preserved_phrase_2,
        },
        "status": "fallback",
    }


def _evidence_level_from_text(raw_text: str) -> str:
    if not raw_text:
        return "weak"
    if ORTHO_HINTS_RE.search(raw_text):
        return "strong"
    return "medium" if hints_braces_or_ortho(raw_text) else "weak"


def _summarize_price_calc(price_calc_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not price_calc_data:
        return {}
    variants = price_calc_data.get("variants") or {}
    summary: Dict[str, Any] = {}
    for key in ("A", "B", "C"):
        v = variants.get(key) or {}
        summary[key] = {
            "total": v.get("total"),
            "fallback_share_pct": v.get("fallback_share_pct"),
            "confidence": v.get("confidence"),
            "missing_items_count": v.get("missing_items_count"),
            "fallback_items_count": v.get("fallback_items_count"),
        }
    return summary


def _attempt_stamp(now: Optional[datetime] = None) -> str:
    dt = now or datetime.now()
    return dt.strftime("%Y%m%d_%H%M%S")


def _repo_root() -> Path:
    # src/outreach/pipeline_outreach.py -> repo root is parents[2]
    return Path(__file__).resolve().parents[2]


def _make_run_dir(out_parent: Path) -> tuple[Path, str]:
    out_parent.mkdir(parents=True, exist_ok=True)
    base_name = _attempt_stamp()
    run_dir = out_parent / base_name
    suffix = 1
    while run_dir.exists():
        run_dir = out_parent / f"{base_name}_{suffix}"
        suffix += 1
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir, run_dir.name


def _parse_summary_pairs(summary_path: Path) -> Dict[str, str]:
    if not summary_path.exists():
        return {}
    pairs: Dict[str, str] = {}
    for raw_line in summary_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = key.strip()
        val = val.strip()
        if key:
            pairs[key] = val
    return pairs


def _write_latest_run_index(
    *,
    out_parent: Path,
    run_dir: Path,
    run_id: str,
    stats: Dict[str, int],
    results_path: Path,
    queue_path: Path,
    summary_path: Path,
    qa_report_path: Path,
    gmail_report_path: Optional[Path],
    max_recent: int = 15,
) -> None:
    out_parent.mkdir(parents=True, exist_ok=True)
    latest_run_path = out_parent / "latest_run.txt"
    latest_dir_path = out_parent / "latest_run_dir.txt"
    latest_md_path = out_parent / "LATEST.md"
    runs_index_path = out_parent / "RUNS_INDEX.md"

    latest_run_path.write_text(run_id, encoding="utf-8")
    latest_dir_path.write_text(str(run_dir), encoding="utf-8")

    gmail_report_display = str(gmail_report_path) if gmail_report_path else ""
    latest_lines: List[str] = []
    latest_lines.append("# Latest Outreach Run")
    latest_lines.append("")
    latest_lines.append(f"- run_id: `{run_id}`")
    latest_lines.append(f"- run_dir: `{run_dir}`")
    latest_lines.append(f"- generated_at: `{_utc_now_iso()}`")
    latest_lines.append("")
    latest_lines.append("## Quick Links")
    latest_lines.append("")
    latest_lines.append(f"- summary: `{summary_path}`")
    latest_lines.append(f"- qa_report: `{qa_report_path}`")
    latest_lines.append(f"- results: `{results_path}`")
    latest_lines.append(f"- queue: `{queue_path}`")
    latest_lines.append(f"- gmail_report: `{gmail_report_display}`")
    latest_lines.append("")
    latest_lines.append("## Key Stats")
    latest_lines.append("")
    latest_lines.append(f"- processed: `{stats.get('processed', 0)}`")
    latest_lines.append(f"- skipped_all_prices_found: `{stats.get('skipped_all_prices_found', 0)}`")
    latest_lines.append(f"- ready_to_email: `{stats.get('ready_to_email', 0)}`")
    latest_lines.append(f"- ready_to_form_assist: `{stats.get('ready_to_form_assist', 0)}`")
    latest_lines.append(f"- manual_needed: `{stats.get('manual_needed', 0)}`")
    latest_lines.append(f"- gemini_invalid_json: `{stats.get('gemini_invalid_json', 0)}`")
    latest_lines.append(f"- gemini_skipped_no_api_key: `{stats.get('gemini_skipped_no_api_key', 0)}`")
    latest_lines.append(f"- gemini_skipped_missing_website: `{stats.get('gemini_skipped_missing_website', 0)}`")
    latest_lines.append(f"- gmail_drafts_created: `{stats.get('gmail_drafts_created', 0)}`")
    atomic_write_text(str(latest_md_path), "\n".join(latest_lines).strip() + "\n")

    recent_runs = _iter_run_dirs(out_parent)[: max(1, int(max_recent))]
    index_lines: List[str] = []
    index_lines.append("# Outreach Runs Index")
    index_lines.append("")
    index_lines.append(f"- generated_at: `{_utc_now_iso()}`")
    index_lines.append(f"- latest_run_id: `{run_id}`")
    index_lines.append(f"- latest_run_dir: `{run_dir}`")
    index_lines.append(f"- latest_pointer: `{latest_md_path}`")
    index_lines.append("")
    index_lines.append("## Recent Runs")
    index_lines.append("")
    index_lines.append(
        "| run_id | processed | ready_to_email | manual_needed | summary | qa_report |"
    )
    index_lines.append("| --- | --- | --- | --- | --- | --- |")
    for recent_dir in recent_runs:
        summary_pairs = _parse_summary_pairs(recent_dir / "outreach_summary.txt")
        processed = summary_pairs.get("processed", "")
        ready = summary_pairs.get("ready_to_email", "")
        manual = summary_pairs.get("manual_needed", "")
        summary_ref = recent_dir / "outreach_summary.txt"
        qa_ref = recent_dir / "QA_REPORT.md"
        index_lines.append(
            f"| {recent_dir.name} | {processed} | {ready} | {manual} | `{summary_ref}` | `{qa_ref}` |"
        )
    atomic_write_text(str(runs_index_path), "\n".join(index_lines).strip() + "\n")


def _iter_run_dirs(out_parent: Path, *, exclude: Optional[Path] = None) -> List[Path]:
    if not out_parent.exists():
        return []
    runs = [p for p in out_parent.iterdir() if p.is_dir()]
    if exclude is not None:
        runs = [p for p in runs if p.resolve() != exclude.resolve()]
    runs.sort(key=lambda p: p.name, reverse=True)
    return runs


def _clinic_gemini_root(run_dir: Path, clinic_slug: str) -> Path:
    return run_dir / "evidence" / clinic_slug / "gemini"


def _latest_attempt_pointer_path(gemini_root: Path) -> Path:
    return gemini_root / "latest_attempt.txt"


def _write_latest_attempt_pointer(gemini_root: Path, attempt_name: str) -> None:
    pointer_path = _latest_attempt_pointer_path(gemini_root)
    pointer_path.write_text(attempt_name, encoding="utf-8")


def _read_latest_attempt_name(gemini_root: Path) -> Optional[str]:
    pointer_path = _latest_attempt_pointer_path(gemini_root)
    if not pointer_path.exists():
        return None
    name = pointer_path.read_text(encoding="utf-8").strip()
    return name or None


def _sorted_attempt_dirs(gemini_root: Path) -> List[Path]:
    if not gemini_root.exists():
        return []
    attempts = [p for p in gemini_root.iterdir() if p.is_dir() and p.name.startswith("attempt_")]
    attempts.sort(key=lambda p: p.name, reverse=True)
    return attempts


def _iter_attempt_dirs(gemini_root: Path) -> List[Path]:
    attempts = _sorted_attempt_dirs(gemini_root)
    latest_name = _read_latest_attempt_name(gemini_root)
    if not latest_name:
        return attempts
    latest_dir = gemini_root / latest_name
    if not latest_dir.exists() or latest_dir not in attempts:
        return attempts
    ordered = [latest_dir]
    ordered.extend(p for p in attempts if p != latest_dir)
    return ordered


def _make_attempt_dir(gemini_root: Path) -> Path:
    gemini_root.mkdir(parents=True, exist_ok=True)
    base_name = f"attempt_{_attempt_stamp()}"
    attempt_dir = gemini_root / base_name
    suffix = 1
    while attempt_dir.exists():
        attempt_dir = gemini_root / f"{base_name}_{suffix}"
        suffix += 1
    attempt_dir.mkdir(parents=True, exist_ok=False)
    _write_latest_attempt_pointer(gemini_root, attempt_dir.name)
    return attempt_dir


def _evidence_paths_for_attempt(gemini_root: Path, prompt_name: str, attempt_dir: Path) -> Dict[str, str]:
    prompt_path = attempt_dir / f"{prompt_name}.prompt.txt"
    raw_path = attempt_dir / f"{prompt_name}.raw.txt"
    meta_path = attempt_dir / f"{prompt_name}.meta.json"
    context_path = attempt_dir / f"{prompt_name}.context.json"
    json_path = attempt_dir / f"{prompt_name}.json"
    raw_pricelist_path = attempt_dir / "raw_pricelist_text.txt"
    latest_pointer_path = _latest_attempt_pointer_path(gemini_root)
    return {
        "gemini_root": str(gemini_root),
        "attempt_dir": str(attempt_dir),
        "attempt_name": attempt_dir.name,
        "latest_attempt_pointer": str(latest_pointer_path),
        "prompt_path": str(prompt_path),
        "raw_path": str(raw_path),
        "meta_path": str(meta_path),
        "context_path": str(context_path),
        "json_path": str(json_path),
        "raw_pricelist_path": str(raw_pricelist_path),
    }


def _is_gmail_skip_name(name: str) -> bool:
    if name in GMAIL_SKIP_EXACT_NAMES:
        return True
    return _is_do_not_contact(name)


def _parse_allow_domains(val: Any) -> set[str]:
    if val is None:
        return set()
    if isinstance(val, str):
        items = [v.strip() for v in val.split(",") if v.strip()]
    elif isinstance(val, (set, frozenset)):
        items = [str(v).strip() for v in val if str(v).strip()]
    elif isinstance(val, Sequence):
        items = [str(v).strip() for v in val if str(v).strip()]
    else:
        items = [str(val).strip()] if str(val).strip() else []
    out: set[str] = set()
    for item in items:
        lowered = item.lower()
        if lowered.startswith("@"):
            lowered = lowered[1:]
        out.add(lowered)
    return out


def _first_recipient_email(emails: List[str]) -> Optional[str]:
    normalized = sorted({(e or "").strip().lower() for e in emails if e})
    if not normalized:
        return None
    return normalized[0]


def _gmail_attempt_paths(clinic_root: Path) -> Dict[str, str]:
    gmail_root = clinic_root / "gmail"
    attempt_dir = _make_attempt_dir(gmail_root)
    latest_pointer = _latest_attempt_pointer_path(gmail_root)
    return {
        "gmail_root": str(gmail_root),
        "attempt_dir": str(attempt_dir),
        "attempt_name": attempt_dir.name,
        "latest_attempt_pointer": str(latest_pointer),
        "to_path": str(attempt_dir / "to.txt"),
        "subject_path": str(attempt_dir / "subject.txt"),
        "body_path": str(attempt_dir / "body.txt"),
        "status_path": str(attempt_dir / "status.json"),
    }


def _write_gmail_attempt_evidence(
    *,
    clinic_root: Path,
    to_email: str,
    subject: str,
    body: str,
    status_payload: Dict[str, Any],
) -> Dict[str, str]:
    paths = _gmail_attempt_paths(clinic_root)
    Path(paths["to_path"]).write_text(to_email or "", encoding="utf-8")
    Path(paths["subject_path"]).write_text(subject or "", encoding="utf-8")
    Path(paths["body_path"]).write_text(body or "", encoding="utf-8")
    Path(paths["status_path"]).write_text(
        json.dumps(status_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return paths


def _render_gmail_report(config: Dict[str, Any], entries: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    lines.append("Outreach Gmail Report")
    lines.append(f"generated_at: {_utc_now_iso()}")
    lines.append(f"drafts_enabled: {bool(config.get('drafts_enabled'))}")
    lines.append(f"gmail_sender_email: {config.get('gmail_sender_email') or ''}")
    lines.append(f"max_drafts: {int(config.get('max_drafts') or 0)}")
    lines.append(f"send_enabled: {bool(config.get('send_enabled'))}")
    lines.append(f"send_ack: {bool(config.get('send_ack'))}")
    lines.append(f"send_dry_run: {bool(config.get('send_dry_run'))}")
    lines.append(f"daily_limit: {int(config.get('daily_limit') or 0)}")
    lines.append(f"label_name: {config.get('label_name') or ''}")
    allow_domains = sorted(list(config.get("allow_domains") or []))
    lines.append(f"allow_domains: {','.join(allow_domains)}")
    lines.append("")
    for entry in entries:
        lines.append(f"- clinic_name: {entry.get('clinic_name')}")
        lines.append(f"  to: {entry.get('to') or ''}")
        lines.append(f"  draft_status: {entry.get('draft_status') or ''}")
        lines.append(f"  draft_id: {entry.get('draft_id') or ''}")
        lines.append(f"  send_status: {entry.get('send_status') or ''}")
        lines.append(f"  reason: {entry.get('reason') or ''}")
        lines.append(f"  attempt_dir: {entry.get('attempt_dir') or ''}")
        lines.append(f"  message_id: {entry.get('message_id') or ''}")
        lines.append(f"  thread_id: {entry.get('thread_id') or ''}")
        lines.append(f"  label_name: {entry.get('label_name') or ''}")
        lines.append(f"  label_applied: {entry.get('label_applied')}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"

def _default_gmail_send_log_path() -> Path:
    return (_repo_root() / "out" / "gmail_send_log.jsonl").resolve()


def _load_send_log_entries(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    entries: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            entries.append(payload)
    return entries


def _has_sent_before(entries: List[Dict[str, Any]], clinic_name: str, to_email: str) -> bool:
    clinic_key = clinic_name.strip().lower()
    email_key = to_email.strip().lower()
    for entry in entries:
        if str(entry.get("clinic_name") or "").strip().lower() != clinic_key:
            continue
        if str(entry.get("to") or "").strip().lower() != email_key:
            continue
        if str(entry.get("status") or "") == "sent":
            return True
    return False


def _count_sent_today(entries: List[Dict[str, Any]], today_key: str) -> int:
    count = 0
    for entry in entries:
        if str(entry.get("date") or "") != today_key:
            continue
        if str(entry.get("status") or "") == "sent":
            count += 1
    return count


def _append_send_log(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _domain_allowed(email: str, allow_domains: set[str]) -> bool:
    if not allow_domains:
        return True
    parts = (email or "").split("@")
    if len(parts) != 2:
        return False
    domain = parts[1].strip().lower()
    return domain in allow_domains


def _write_qa_report(
    *,
    run_dir: Path,
    run_id: str,
    input_path: Path,
    results: List[Dict[str, Any]],
    gmail_report_path: Optional[Path],
    preflight_info: Optional[Dict[str, Any]],
) -> Path:
    qa_path = run_dir / "QA_REPORT.md"
    lines: List[str] = []
    lines.append("# Outreach QA Report")
    lines.append("")
    lines.append(f"- run_id: `{run_id}`")
    lines.append(f"- run_dir: `{run_dir}`")
    lines.append(f"- outreach_input: `{input_path}`")
    lines.append(f"- generated_at: `{_utc_now_iso()}`")
    if gmail_report_path is not None:
        lines.append(f"- gmail_report: `{gmail_report_path}`")
    lines.append("")

    if preflight_info:
        lines.append("## Preflight Summary")
        lines.append("")
        for key, val in preflight_info.items():
            lines.append(f"- {key}: `{val}`")
        lines.append("")

    lines.append("## Clinic Summary")
    lines.append("")
    lines.append(
        "| Clinic | Website? | Pricing Found? | Emails Found? | Form Found? | Gemini Status | Final Action | Draft Created? |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    failures: List[str] = []

    for row in results:
        clinic = str(row.get("clinic_name") or row.get("name") or "")
        website_present = "yes" if row.get("website_url") else "no"
        pricing_status = str((row.get("pricing") or {}).get("pricing_status") or "")
        pricing_found = "yes" if pricing_status in {"html_text", "partial", "pdf_only"} else "no"
        emails_found = "yes" if (row.get("discovered") or {}).get("emails") else "no"
        form_found = "yes" if (row.get("discovered") or {}).get("forms") else "no"
        gemini_status = row.get("gemini_status") or {}
        gemini_outreach = str(gemini_status.get("outreach") or "")
        final_action = str((row.get("suggested_action") or {}).get("status") or row.get("status") or "")
        gmail_draft = row.get("gmail_draft") or {}
        draft_created = "yes" if gmail_draft.get("draft_id") else "no"

        lines.append(
            f"| {clinic} | {website_present} | {pricing_found} | {emails_found} | {form_found} | {gemini_outreach} | {final_action} | {draft_created} |"
        )

        if website_present == "no":
            failures.append(f"{clinic}: missing website URL in input.")
        if gemini_outreach.startswith("invalid_json"):
            failures.append(f"{clinic}: Gemini returned invalid JSON.")
        if gemini_outreach.startswith("request_error"):
            failures.append(f"{clinic}: Gemini request error (likely network/DNS).")
        if final_action in {"manual_needed", ""}:
            failures.append(f"{clinic}: manual action required ({(row.get('suggested_action') or {}).get('reason')}).")

    lines.append("")
    lines.append("## Failures And Root Causes")
    lines.append("")
    if failures:
        for item in failures:
            lines.append(f"- {item}")
    else:
        lines.append("- None detected in this run.")
    lines.append("")

    lines.append("## Commands To Run")
    lines.append("")
    lines.append("Draft mode (safe, no sends):")
    lines.append("")
    lines.append("```bash")
    lines.append(
        f"python3 run.py --outreach --outreach-input \"{input_path}\" --outreach-top-n 5 --outreach-max-pages 10 --outreach-out out/outreach_runs --gmail-drafts --gmail-max-drafts 3"
    )
    lines.append("```")
    lines.append("")
    lines.append("Auto-send mode (requires explicit acknowledgement):")
    lines.append("")
    lines.append("```bash")
    lines.append(
        f"python3 run.py --outreach --outreach-input \"{input_path}\" --outreach-top-n 5 --outreach-max-pages 10 --outreach-out out/outreach_runs --gmail-send --i-understand-this-will-send-email --gmail-send-no-dry-run --gmail-daily-limit 10 --allow-domains \"clinic.pl\""
    )
    lines.append("```")
    lines.append("")

    lines.append("## Review Checklist Before Auto-Send")
    lines.append("")
    lines.append("- Confirm the clinic websites and recipient emails are correct.")
    lines.append("- Read the generated subject and body for each draft.")
    lines.append("- Verify do-not-contact clinics are skipped.")
    lines.append("- Ensure allow-domains matches the intended recipients.")
    lines.append("- Keep dry-run enabled until you have reviewed drafts.")
    lines.append("")

    atomic_write_text(str(qa_path), "\n".join(lines))
    return qa_path


def _process_gmail_actions(
    *,
    results: List[Dict[str, Any]],
    queue_rows: List[Dict[str, Any]],
    out_dir: Path,
    run_id: str,
    gmail_drafts: bool,
    gmail_sender_email: str,
    gmail_max_drafts: int,
    gmail_send: bool,
    gmail_send_ack: bool,
    gmail_send_dry_run: bool,
    gmail_daily_limit: int,
    gmail_allow_domains: Any,
    gmail_send_log_path: Optional[Path],
    gmail_sender: Optional[Any],
) -> Dict[str, int]:
    stats = {
        "gmail_candidates": 0,
        "gmail_drafts_created": 0,
        "gmail_attempted": 0,
        "gmail_sent": 0,
        "gmail_blocked": 0,
    }
    if not gmail_drafts and not gmail_send:
        return stats

    gmail_sender_email = str(gmail_sender_email or "").strip()
    gmail_max_drafts = max(0, int(gmail_max_drafts))
    gmail_send = bool(gmail_send)
    gmail_send_ack = bool(gmail_send_ack)
    gmail_send_dry_run = bool(gmail_send_dry_run)
    gmail_daily_limit = max(0, int(gmail_daily_limit))
    allow_domains = _parse_allow_domains(gmail_allow_domains)
    gmail_label_name = DEFAULT_GMAIL_LABEL

    queue_by_place_id: Dict[str, Dict[str, Any]] = {}
    queue_by_name: Dict[str, Dict[str, Any]] = {}
    for row in queue_rows:
        place_id = str(row.get("place_id") or "").strip()
        name = str(row.get("clinic_name") or "").strip()
        if place_id:
            queue_by_place_id[place_id] = row
        if name:
            queue_by_name[name] = row

    sender_error: Optional[str] = None
    if (gmail_drafts or gmail_send) and gmail_sender is None:
        try:
            from src.gmail_sender import GmailSender

            gmail_sender = GmailSender()
        except Exception as exc:  # pragma: no cover - depends on optional deps/config
            sender_error = f"sender_init_error: {exc}"

    if gmail_sender is not None and not gmail_sender_email:
        try:
            gmail_sender_email = str(gmail_sender.get_profile_email() or "")
        except Exception:
            gmail_sender_email = ""

    send_log_path = gmail_send_log_path or _default_gmail_send_log_path()
    send_log_entries = _load_send_log_entries(send_log_path) if gmail_send else []
    today_key = datetime.now().date().isoformat()
    sent_today = _count_sent_today(send_log_entries, today_key) if gmail_send else 0
    send_remaining_today = max(0, gmail_daily_limit - sent_today)

    drafts_created = 0
    sent_this_run = 0
    report_entries: List[Dict[str, Any]] = []

    for result in results:
        suggested = (result.get("suggested_action") or {}).get("status")
        if suggested != "ready_to_email":
            continue
        stats["gmail_candidates"] += 1

        clinic_name = str(result.get("clinic_name") or "").strip()
        if _is_gmail_skip_name(clinic_name):
            report_entries.append(
                {
                    "clinic_name": clinic_name,
                    "status": "skipped_do_not_contact",
                    "reason": "user_blocklist",
                }
            )
            stats["gmail_blocked"] += 1
            continue

        place_id = str(result.get("place_id") or "").strip()
        queue_row = queue_by_place_id.get(place_id) or queue_by_name.get(clinic_name) or {}

        discovered = result.get("discovered") or {}
        emails = list(discovered.get("emails") or queue_row.get("emails") or [])
        to_email = _first_recipient_email(emails)

        outreach_msg = (result.get("gemini") or {}).get("outreach_message") or {}
        subject = str(outreach_msg.get("subject") or "").strip()
        body = str(outreach_msg.get("body") or "").strip()

        clinic_root = Path((result.get("evidence") or {}).get("clinic_root") or "")
        if not clinic_root:
            # If evidence is missing we cannot write audit trails reliably.
            report_entries.append(
                {
                    "clinic_name": clinic_name,
                    "status": "blocked_missing_evidence_dir",
                    "reason": "missing_clinic_root",
                    "to": to_email or "",
                    "subject": subject,
                }
            )
            stats["gmail_blocked"] += 1
            continue

        reason = ""
        draft_reason = ""
        send_reason = ""
        send_status = ""
        draft_status = ""
        draft_id = ""
        message_id = ""
        thread_id = ""
        label_id = ""
        label_applied = False
        label_error = ""
        send_error = ""

        if not to_email or not subject or not body:
            draft_status = "blocked_missing_message_fields"
            send_status = "blocked_missing_message_fields"
            draft_reason = "missing_to_subject_or_body"
            send_reason = "missing_to_subject_or_body"
        if gmail_drafts and not draft_reason:
            if drafts_created >= gmail_max_drafts:
                draft_status = "blocked_max_drafts"
                draft_reason = f"max_drafts_reached:{gmail_max_drafts}"
            elif sender_error or gmail_sender is None:
                draft_status = "blocked_sender_error"
                draft_reason = sender_error or "sender_missing"
            else:
                try:
                    resp = gmail_sender.create_draft(
                        to_email=to_email,
                        subject=subject,
                        body=body,
                        sender_email=gmail_sender_email,
                        label_name=gmail_label_name,
                    )
                    draft_status = str(resp.get("status") or "")
                    draft_id = str(resp.get("draft_id") or "")
                    message_id = str(resp.get("message_id") or message_id)
                    thread_id = str(resp.get("thread_id") or resp.get("threadId") or thread_id)
                    label_id = str(resp.get("label_id") or label_id)
                    label_applied = bool(resp.get("label_applied") or label_applied)
                    if resp.get("label_error"):
                        label_error = str(resp.get("label_error") or "")
                    if draft_status == "drafted" and draft_id:
                        drafts_created += 1
                        stats["gmail_drafts_created"] += 1
                except Exception as exc:  # pragma: no cover - depends on external API
                    draft_status = "error"
                    draft_reason = f"draft_exception:{exc}"

        if gmail_drafts and not gmail_send and draft_status not in {"", "drafted"}:
            stats["gmail_blocked"] += 1

        if gmail_send:
            if send_reason:
                send_status = send_status or "blocked_missing_message_fields"
            elif not gmail_send_ack:
                send_status = "blocked_ack_required"
                send_reason = "missing_ack_flag"
            elif gmail_send_dry_run:
                send_status = "dry_run"
                send_reason = "send_dry_run_enabled"
            elif not _domain_allowed(to_email, allow_domains):
                send_status = "blocked_domain_not_allowed"
                send_reason = f"domain_not_in_allowlist:{to_email.split('@')[-1]}"
            elif _has_sent_before(send_log_entries, clinic_name, to_email):
                send_status = "blocked_dedupe"
                send_reason = "already_sent_before"
            elif sent_this_run >= send_remaining_today:
                send_status = "blocked_daily_limit"
                send_reason = f"daily_limit_reached:{gmail_daily_limit}"
            elif sender_error or gmail_sender is None:
                send_status = "blocked_sender_error"
                send_reason = sender_error or "sender_missing"
            else:
                try:
                    resp = gmail_sender.send_email(
                        to_email=to_email,
                        subject=subject,
                        body=body,
                        dry_run=False,
                        label_name=gmail_label_name,
                    )
                    send_status = str(resp.get("status") or "")
                    message_id = str(resp.get("message_id") or "")
                    thread_id = str(resp.get("thread_id") or resp.get("threadId") or thread_id)
                    label_id = str(resp.get("label_id") or label_id)
                    label_applied = bool(resp.get("label_applied") or label_applied)
                    if resp.get("label_error"):
                        label_error = str(resp.get("label_error") or "")
                except Exception as exc:  # pragma: no cover - depends on external API
                    send_status = "error"
                    send_error = str(exc)
                    send_reason = f"send_exception:{exc}"

            stats["gmail_attempted"] += 1
            if send_status == "sent" and message_id:
                stats["gmail_sent"] += 1
                sent_this_run += 1
                log_payload = {
                    "clinic_name": clinic_name,
                    "to": to_email,
                    "date": today_key,
                    "status": "sent",
                    "message_id": message_id,
                    "thread_id": thread_id,
                    "run_id": run_id,
                    "sent_at": _utc_now_iso(),
                }
                _append_send_log(send_log_path, log_payload)
                send_log_entries.append(log_payload)
            elif send_status not in {"sent"}:
                stats["gmail_blocked"] += 1

        reason = send_reason if gmail_send else draft_reason

        status_payload = {
            "clinic_name": clinic_name,
            "place_id": place_id,
            "draft_status": draft_status,
            "draft_id": draft_id,
            "send_status": send_status,
            "reason": reason,
            "draft_reason": draft_reason,
            "send_reason": send_reason,
            "to": to_email or "",
            "subject": subject,
            "message_id": message_id,
            "thread_id": thread_id,
            "error": send_error,
            "label_name": gmail_label_name,
            "label_id": label_id,
            "label_applied": label_applied,
            "label_error": label_error,
            "gmail_sender_email": gmail_sender_email,
            "drafts_enabled": bool(gmail_drafts),
            "max_drafts": gmail_max_drafts,
            "send_enabled": bool(gmail_send),
            "send_ack": gmail_send_ack,
            "send_dry_run": gmail_send_dry_run,
            "daily_limit": gmail_daily_limit,
            "allow_domains": sorted(list(allow_domains)),
            "outreach_force": bool(result.get("outreach_force")),
            "attempted_at": _utc_now_iso(),
        }
        evidence_paths = _write_gmail_attempt_evidence(
            clinic_root=clinic_root,
            to_email=to_email or "",
            subject=subject,
            body=body,
            status_payload=status_payload,
        )

        gmail_draft_result = {
            "enabled": bool(gmail_drafts),
            "status": draft_status,
            "draft_id": draft_id,
            "to": to_email or "",
            "message_id": message_id,
            "thread_id": thread_id,
            "label_name": gmail_label_name,
            "label_id": label_id,
            "label_applied": label_applied,
            "label_error": label_error,
            "reason": reason,
            "attempt_dir": evidence_paths.get("attempt_dir") or "",
            "attempt_name": evidence_paths.get("attempt_name") or "",
            "latest_attempt_pointer": evidence_paths.get("latest_attempt_pointer") or "",
            "subject_path": evidence_paths.get("subject_path") or "",
            "body_path": evidence_paths.get("body_path") or "",
            "to_path": evidence_paths.get("to_path") or "",
            "status_path": evidence_paths.get("status_path") or "",
        }
        gmail_send_result = {
            "enabled": bool(gmail_send),
            "ack": bool(gmail_send_ack),
            "dry_run": bool(gmail_send_dry_run),
            "daily_limit": gmail_daily_limit,
            "allow_domains": sorted(list(allow_domains)),
            "status": send_status,
            "reason": reason,
            "to": to_email or "",
            "message_id": message_id,
            "thread_id": thread_id,
            "label_name": gmail_label_name,
            "label_id": label_id,
            "label_applied": label_applied,
            "label_error": label_error,
            "error": send_error or "",
            "attempt_dir": evidence_paths.get("attempt_dir") or "",
            "attempt_name": evidence_paths.get("attempt_name") or "",
            "latest_attempt_pointer": evidence_paths.get("latest_attempt_pointer") or "",
            "status_path": evidence_paths.get("status_path") or "",
        }
        result["gmail_draft"] = gmail_draft_result
        result["gmail_send"] = gmail_send_result
        if queue_row is not None:
            queue_row["gmail_draft"] = gmail_draft_result
            queue_row["gmail_send"] = gmail_send_result

        if gmail_send and send_status not in {"sent", ""}:
            # Sending was requested but not completed; require manual review.
            result.setdefault("suggested_action", {})
            result["suggested_action"]["status"] = "manual_needed"
            result["suggested_action"]["reason"] = f"gmail_blocked:{reason or send_status}"
            if queue_row is not None:
                queue_row["status"] = "manual_needed"
                queue_row["reason"] = f"gmail_blocked:{reason or send_status}"
        elif gmail_send and send_status == "sent":
            result.setdefault("suggested_action", {})
            result["suggested_action"]["status"] = "sent_gmail"
            result["suggested_action"]["reason"] = "gmail_sent"
            if queue_row is not None:
                queue_row["status"] = "sent_gmail"
                queue_row["reason"] = "gmail_sent"

        report_entries.append(
            {
                "clinic_name": clinic_name,
                "draft_status": draft_status,
                "draft_id": draft_id,
                "send_status": send_status,
                "reason": reason,
                "to": to_email or "",
                "subject": subject,
                "attempt_dir": evidence_paths.get("attempt_dir") or "",
                "message_id": message_id,
                "thread_id": thread_id,
                "label_name": gmail_label_name,
                "label_applied": label_applied,
            }
        )

    report_config = {
        "drafts_enabled": bool(gmail_drafts),
        "gmail_sender_email": gmail_sender_email,
        "max_drafts": gmail_max_drafts,
        "send_enabled": bool(gmail_send),
        "send_ack": bool(gmail_send_ack),
        "send_dry_run": bool(gmail_send_dry_run),
        "daily_limit": gmail_daily_limit,
        "label_name": gmail_label_name,
        "allow_domains": sorted(list(allow_domains)),
    }
    report_text = _render_gmail_report(report_config, report_entries)
    report_path = out_dir / "outreach_gmail_report.txt"
    atomic_write_text(str(report_path), report_text)
    return stats


def _load_latest_ok_gemini(
    gemini_root: Path,
    prompt_name: str,
    expected_prompt_hash: str,
    raw_pricelist_text: Optional[str],
) -> Optional[Dict[str, Any]]:
    expected_prompt_hash = str(expected_prompt_hash or "")
    expected_raw_hash = hash_text(raw_pricelist_text) if raw_pricelist_text else ""
    for attempt_dir in _iter_attempt_dirs(gemini_root):
        meta_path = attempt_dir / f"{prompt_name}.meta.json"
        json_path = attempt_dir / f"{prompt_name}.json"
        if not meta_path.exists() or not json_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(meta, dict) or not isinstance(data, dict):
            continue
        if meta.get("status") != "ok":
            continue
        meta_prompt_hash = str(meta.get("prompt_hash") or "")
        if meta_prompt_hash and expected_prompt_hash and meta_prompt_hash != expected_prompt_hash:
            continue
        meta_raw_hash = str(meta.get("raw_pricelist_hash") or "")
        if meta_raw_hash and expected_raw_hash and meta_raw_hash != expected_raw_hash:
            continue
        return {
            "attempt_dir": attempt_dir,
            "meta": meta,
            "data": data,
            "prompt_hash": meta_prompt_hash or expected_prompt_hash,
            "raw_pricelist_hash": meta_raw_hash or expected_raw_hash,
        }
    return None


def _write_gemini_evidence(
    *,
    evidence_dir: Path,
    prompt_name: str,
    prompt_text: str,
    prompt_hash: str,
    raw_pricelist_text: Optional[str],
    result: GeminiCallResult,
    context_payload: Dict[str, Any],
) -> Dict[str, str]:
    attempt_dir = _make_attempt_dir(evidence_dir)
    paths = _evidence_paths_for_attempt(evidence_dir, prompt_name, attempt_dir)

    prompt_path = Path(paths["prompt_path"])
    raw_path = Path(paths["raw_path"])
    meta_path = Path(paths["meta_path"])
    context_path = Path(paths["context_path"])
    json_path = Path(paths["json_path"])
    raw_pricelist_path = Path(paths["raw_pricelist_path"])

    prompt_path.write_text(prompt_text, encoding="utf-8")
    raw_path.write_text(result.raw_text or "", encoding="utf-8")
    raw_pricelist_hash = hash_text(raw_pricelist_text) if raw_pricelist_text else ""
    meta = {
        "prompt_name": prompt_name,
        "prompt_hash": prompt_hash,
        "attempt_name": attempt_dir.name,
        "status": result.status,
        "model": result.model,
        "error": result.error,
        "raw_pricelist_hash": raw_pricelist_hash,
        "generated_at": _utc_now_iso(),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    context_path.write_text(json.dumps(context_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if raw_pricelist_text is not None:
        raw_pricelist_path.write_text(raw_pricelist_text, encoding="utf-8")

    if isinstance(result.data, dict):
        json_path.write_text(json.dumps(result.data, ensure_ascii=False, indent=2), encoding="utf-8")

    paths["status"] = result.status
    paths["prompt_hash"] = prompt_hash
    paths["raw_pricelist_hash"] = raw_pricelist_hash

    return paths


def _write_template_outreach_evidence(
    *,
    clinic_root: Path,
    template_result: Dict[str, Any],
) -> Dict[str, str]:
    template_root = clinic_root / "outreach_template"
    attempt_dir = _make_attempt_dir(template_root)
    subject_path = attempt_dir / "subject.txt"
    body_path = attempt_dir / "body.txt"
    meta_path = attempt_dir / "meta.json"

    subject_path.write_text(str(template_result.get("subject") or ""), encoding="utf-8")
    body_path.write_text(str(template_result.get("body") or ""), encoding="utf-8")
    meta_path.write_text(json.dumps(template_result, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "template_root": str(template_root),
        "attempt_dir": str(attempt_dir),
        "attempt_name": attempt_dir.name,
        "latest_attempt_pointer": str(_latest_attempt_pointer_path(template_root)),
        "subject_path": str(subject_path),
        "body_path": str(body_path),
        "meta_path": str(meta_path),
    }


def _infer_run_id_from_attempt(attempt_dir: Path) -> str:
    try:
        # attempt -> gemini -> clinic_slug -> evidence -> run_dir
        return attempt_dir.resolve().parents[3].name
    except Exception:
        return ""


def _copy_cached_attempt_into_current_run(
    *,
    cached: Dict[str, Any],
    dest_gemini_root: Path,
    prompt_name: str,
    source_run_id: str,
) -> Dict[str, Any]:
    src_attempt_dir = Path(cached.get("attempt_dir") or "")
    if not src_attempt_dir.exists():
        return cached

    dest_attempt_dir = _make_attempt_dir(dest_gemini_root)
    src_paths = _evidence_paths_for_attempt(src_attempt_dir.parent, prompt_name, src_attempt_dir)
    dest_paths = _evidence_paths_for_attempt(dest_gemini_root, prompt_name, dest_attempt_dir)

    for key in ("prompt_path", "raw_path", "context_path", "json_path", "raw_pricelist_path"):
        src_path = Path(src_paths[key])
        dest_path = Path(dest_paths[key])
        if src_path.exists():
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, dest_path)

    meta_payload: Dict[str, Any] = dict(cached.get("meta") or {})
    meta_payload["status"] = "ok_cached"
    meta_payload["attempt_name"] = dest_attempt_dir.name
    meta_payload["cached_from_attempt"] = src_attempt_dir.name
    if source_run_id:
        meta_payload["cached_from_run_id"] = source_run_id
    meta_payload["cached_at"] = _utc_now_iso()

    meta_path = Path(dest_paths["meta_path"])
    meta_path.write_text(json.dumps(meta_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "attempt_dir": dest_attempt_dir,
        "meta": meta_payload,
        "data": cached.get("data"),
        "prompt_hash": str(meta_payload.get("prompt_hash") or cached.get("prompt_hash") or ""),
        "raw_pricelist_hash": str(
            meta_payload.get("raw_pricelist_hash") or cached.get("raw_pricelist_hash") or ""
        ),
        "source_run_id": source_run_id,
    }


def _load_latest_ok_gemini_across_runs(
    *,
    out_parent: Path,
    run_dir: Path,
    clinic_slug: str,
    prompt_name: str,
    prompt_hash: str,
    raw_pricelist_text: Optional[str],
) -> Optional[Dict[str, Any]]:
    current_root = _clinic_gemini_root(run_dir, clinic_slug)
    cached = _load_latest_ok_gemini(current_root, prompt_name, prompt_hash, raw_pricelist_text)
    if cached:
        cached["source_run_id"] = run_dir.name
        return cached

    for prior_run_dir in _iter_run_dirs(out_parent, exclude=run_dir):
        prior_root = _clinic_gemini_root(prior_run_dir, clinic_slug)
        cached = _load_latest_ok_gemini(prior_root, prompt_name, prompt_hash, raw_pricelist_text)
        if not cached:
            continue
        cached["source_run_id"] = prior_run_dir.name
        return cached
    return None


def _choose_form_candidate(forms: List[str]) -> Optional[str]:
    if not forms:
        return None
    contact_forms = [f for f in forms if is_contact_url(f)]
    if contact_forms:
        return sorted(contact_forms)[0]
    return sorted(forms)[0]


def run_outreach(
    *,
    input_csv_path: Optional[str] = None,
    out_dir: str | Path = DEFAULT_OUTREACH_DIR,
    top_n: int = 30,
    max_pages: int = DEFAULT_MAX_PAGES,
    refresh_web: bool = False,
    refresh_places: bool = False,
    playwright_assist: bool = False,
    playwright_headed: bool = False,
    playwright_slowmo_ms: int = 0,
    outreach_force: bool = False,
    gmail_drafts: bool = False,
    gmail_sender_email: str = "",
    gmail_max_drafts: int = 5,
    gmail_send: bool = False,
    gmail_send_ack: bool = False,
    gmail_send_dry_run: bool = True,
    gmail_daily_limit: int = 10,
    gmail_allow_domains: Any = None,
    gmail_send_log_path: Optional[str | Path] = None,
    preflight_info: Optional[Dict[str, Any]] = None,
    # Backward-compatible flags (deprecated; mapped to Gmail send rails).
    outreach_send_gmail: bool = False,
    outreach_send_confirm: str = "",
    outreach_send_max: int = 5,
    outreach_send_dry_run: bool = True,
    gmail_sender: Optional[Any] = None,
    fetcher: Optional[Callable[[str], Any]] = None,
    gemini_client: Optional[BaseGeminiClient] = None,
    template_path: Path = EMAIL_TEMPLATE_PATH,
    price_prompt_path: Path = PROMPT_PRICE_CALC_PATH,
    outreach_prompt_path: Path = PROMPT_OUTREACH_PATH,
) -> OutreachRunResult:
    out_parent = Path(out_dir)

    input_path: Optional[Path]
    if input_csv_path:
        input_path = Path(input_csv_path)
    else:
        input_path = find_latest_merged_csv()
    if not input_path or not input_path.exists():
        raise FileNotFoundError(
            "Outreach input not found. Provide --outreach-input (CSV or JSON) or run a radius scan first."
        )

    # Always create a non-destructive run directory under the provided parent.
    run_dir, run_id = _make_run_dir(out_parent)
    out_dir = run_dir

    evidence_root = out_dir / "evidence"
    cache_dir = out_dir / "cache" / "pages"
    evidence_root.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    rows, preserve_rank_order = _read_input_rows(input_path)
    rows = _filter_rows_with_name(rows)
    if preserve_rank_order:
        rows_ranked = rows
    else:
        rows_ranked = sorted(rows, key=_sort_key, reverse=True)
    rows_top = _dedup_rows(rows_ranked)[: max(1, int(top_n))]

    gemini = gemini_client or GeminiClient.from_env()

    email_template_text = _load_text(template_path)
    price_prompt_template = _load_text(price_prompt_path)
    price_prompt_hash = hash_text(price_prompt_template)

    refresh_web_effective = bool(refresh_web or outreach_force)
    refresh_places_effective = bool(refresh_places or outreach_force)
    gmail_drafts = bool(gmail_drafts)
    gmail_sender_email = str(gmail_sender_email or "").strip()
    gmail_max_drafts = max(0, int(gmail_max_drafts))
    gmail_send = bool(gmail_send)
    gmail_send_ack = bool(gmail_send_ack)
    gmail_send_dry_run = bool(gmail_send_dry_run)
    gmail_daily_limit = max(0, int(gmail_daily_limit))
    gmail_send_log_path = Path(gmail_send_log_path).resolve() if gmail_send_log_path else None
    gmail_allow_domains_set = _parse_allow_domains(gmail_allow_domains)

    # Map deprecated flags into the new Gmail send configuration.
    if outreach_send_gmail:
        gmail_send = True
        gmail_daily_limit = max(0, int(outreach_send_max))
    if str(outreach_send_confirm or "").strip().upper() == "SEND":
        gmail_send_ack = True
    # Both dry-run flags must be explicitly disabled before sending.
    gmail_send_dry_run = bool(gmail_send_dry_run and outreach_send_dry_run)

    cache = PageCache(cache_dir)
    crawler = DomainLimitedCrawler(max_pages=max_pages, refresh_web=refresh_web_effective, cache=cache)

    results: List[Dict[str, Any]] = []
    queue_rows: List[Dict[str, Any]] = []

    stats = {
        "processed": 0,
        "skipped_do_not_contact": 0,
        "skipped_all_prices_found": 0,
        "needs_manual_search": 0,
        "ready_to_email": 0,
        "ready_to_form_assist": 0,
        "manual_needed": 0,
        "pricing_html_text": 0,
        "pricing_pdf_only": 0,
        "pricing_none": 0,
        "gemini_invalid_json": 0,
        "gemini_skipped_no_api_key": 0,
        "gemini_skipped_missing_website": 0,
        "gemini_cached_ok": 0,
        "gmail_candidates": 0,
        "gmail_drafts_created": 0,
        "gmail_attempted": 0,
        "gmail_sent": 0,
        "gmail_blocked": 0,
    }

    for rank, row in enumerate(rows_top, start=1):
        name = str(row.get("name") or "Unknown Clinic").strip()
        place_id = str(row.get("place_id") or "").strip()
        clinic_slug = _clinic_slug(row, rank)
        evidence_dirs = _ensure_evidence_dirs(evidence_root, clinic_slug)

        if _is_do_not_contact(name):
            stats["skipped_do_not_contact"] += 1
            result = {
                "rank": rank,
                "clinic_name": name,
                "place_id": place_id,
                "clinic_slug": clinic_slug,
                "status": "skipped_do_not_contact",
                "reason": "user_blocklist",
                "evidence_dir": str(evidence_dirs.clinic_root),
                "run_id": run_id,
                "run_dir": str(run_dir),
                "gmail_drafts_enabled": gmail_drafts,
                "gmail_send_enabled": gmail_send,
                "outreach_force": bool(outreach_force),
            }
            results.append(result)
            queue_rows.append(
                {
                    "clinic_name": name,
                    "place_id": place_id,
                    "status": "skipped_do_not_contact",
                    "reason": "user_blocklist",
                    "manual_review_required": True,
                    "run_id": run_id,
                    "run_dir": str(run_dir),
                    "gmail_drafts_enabled": gmail_drafts,
                    "gmail_send_enabled": gmail_send,
                    "outreach_force": bool(outreach_force),
                }
            )
            continue

        website_url = _pick_website(row)
        needs_manual_search = not bool(website_url)
        can_run_gemini = not needs_manual_search

        crawl_result: Optional[CrawlResult] = None
        pricing_pages: List[Any] = []
        contact_pages: List[Any] = []
        emails: List[str] = []
        forms: List[str] = []
        pdf_urls: List[str] = []
        pdf_paths: List[str] = []
        pricing_text = ""
        pricing_text_chars = 0
        pricing_text_sufficient = False
        pricing_status = "none"

        price_calc_result: Optional[GeminiCallResult] = None
        price_calc_data: Optional[Dict[str, Any]] = None
        outreach_data: Optional[Dict[str, Any]] = None
        assist_result: Optional[AssistResult] = None

        crawl_errors: List[str] = []
        cache_hits = 0
        live_fetches = 0

        if needs_manual_search:
            stats["needs_manual_search"] += 1
        else:
            crawl_result = crawler.crawl(website_url, evidence_dirs.pages_dir, fetcher=fetcher)
            crawl_errors = crawl_result.errors
            cache_hits = crawl_result.cache_hits
            live_fetches = crawl_result.live_fetches

            pricing_pages = [p for p in crawl_result.pages if _is_pricing_page(p)]
            contact_pages = [p for p in crawl_result.pages if _is_contact_page(p)]
            pdf_urls = _collect_pdf_urls(crawl_result.pages)
            forms = sorted({f for p in crawl_result.pages for f in (p.forms or [])})

            raw_texts = [p.text for p in crawl_result.pages if p.text]
            mailto_emails = [e for p in crawl_result.pages for e in (p.mailto_emails or [])]
            emails = extract_emails(raw_texts, extra_emails=mailto_emails)

            if pricing_pages:
                pricing_text = _pricing_text_from_pages(pricing_pages)
                pricing_text_chars = len(pricing_text)
                if pricing_text:
                    evidence_dirs.extracted_pricing_path.write_text(pricing_text, encoding="utf-8")
                pricing_text_sufficient = pricing_text_chars >= PRICING_TEXT_MIN_CHARS

            if pdf_urls:
                session = crawler.session
                for pdf_url in pdf_urls[:10]:
                    path = _download_pdf(pdf_url, evidence_dirs.pdf_dir, session=session)
                    if path:
                        pdf_paths.append(path)

            if pdf_paths and not pricing_text:
                pricing_status = "pdf_only"
            elif pricing_text:
                pricing_status = "html_text" if pricing_text_sufficient else "partial"

        raw_pricelist_text = pricing_text
        if pricing_status == "pdf_only" and not raw_pricelist_text:
            raw_pricelist_text = (
                "PDF pricing documents were found but not OCR'd. "
                + "PDF paths: "
                + json.dumps(pdf_paths, ensure_ascii=False)
            )

        # Gemini price calc runs when we have any pricing evidence or PDFs.
        price_evidence_paths: Dict[str, str] = {}
        if can_run_gemini and raw_pricelist_text:
            price_prompt_text = _replace_tokens(
                price_prompt_template,
                {
                    "CLINIC_NAME": name,
                    "PRICE_LIST_TEXT": raw_pricelist_text,
                },
            )
            gemini_context = {
                "clinic_name": name,
                "pricing_status": pricing_status,
                "pricing_text_chars": pricing_text_chars,
                "pdf_paths": pdf_paths,
            }
            cached_price: Optional[Dict[str, Any]] = None
            if not outreach_force:
                cached_price = _load_latest_ok_gemini_across_runs(
                    out_parent=out_parent,
                    run_dir=run_dir,
                    clinic_slug=clinic_slug,
                    prompt_name=price_prompt_path.name,
                    prompt_hash=price_prompt_hash,
                    raw_pricelist_text=raw_pricelist_text,
                )
                if cached_price:
                    src_attempt_dir = Path(cached_price.get("attempt_dir") or "")
                    if src_attempt_dir.exists() and src_attempt_dir.parent.resolve() != evidence_dirs.gemini_dir.resolve():
                        source_run_id = str(cached_price.get("source_run_id") or _infer_run_id_from_attempt(src_attempt_dir))
                        cached_price = _copy_cached_attempt_into_current_run(
                            cached=cached_price,
                            dest_gemini_root=evidence_dirs.gemini_dir,
                            prompt_name=price_prompt_path.name,
                            source_run_id=source_run_id,
                        )
            if cached_price:
                cached_hash = str(cached_price.get("prompt_hash") or price_prompt_hash)
                price_calc_data = cached_price.get("data")
                price_calc_result = GeminiCallResult(
                    status="ok_cached",
                    raw_text="",
                    data=price_calc_data,
                    model="cached",
                    prompt_name=price_prompt_path.name,
                    prompt_hash=cached_hash,
                    error=None,
                )
                stats["gemini_cached_ok"] += 1
                price_evidence_paths = _evidence_paths_for_attempt(
                    evidence_dirs.gemini_dir, price_prompt_path.name, cached_price["attempt_dir"]
                )
                price_evidence_paths["status"] = "ok_cached"
                price_evidence_paths["prompt_hash"] = cached_hash
            else:
                price_calc_result = gemini.generate_json(
                    prompt_name=price_prompt_path.name,
                    prompt_text=price_prompt_text,
                    prompt_hash=price_prompt_hash,
                    validator=_validate_price_calc,
                )
                if price_calc_result.status == "ok":
                    price_calc_data = price_calc_result.data
                elif price_calc_result.status == "invalid_json":
                    stats["gemini_invalid_json"] += 1
                elif price_calc_result.status == "skipped_no_api_key":
                    stats["gemini_skipped_no_api_key"] += 1

                price_evidence_paths = _write_gemini_evidence(
                    evidence_dir=evidence_dirs.gemini_dir,
                    prompt_name=price_prompt_path.name,
                    prompt_text=price_prompt_text,
                    prompt_hash=price_prompt_hash,
                    raw_pricelist_text=raw_pricelist_text,
                    result=price_calc_result,
                    context_payload=gemini_context,
                )

        missing_prices = _collect_missing_prices(price_calc_data)
        if price_calc_data:
            missing_prices = missing_prices
        if pricing_status == "html_text" and missing_prices:
            # If we could not confirm pricing completeness (or price-calc failed),
            # treat as partial so outreach messaging still runs.
            pricing_status = "partial"

        # Template-based outreach (no Gemini).
        should_generate_outreach = (not needs_manual_search) and pricing_status in {"none", "partial", "pdf_only"}
        outreach_evidence_paths: Dict[str, str] = {}
        template_result: Optional[Dict[str, Any]] = None
        if should_generate_outreach:
            template_result = _render_template_outreach(
                clinic_name=name,
                clinic_website=website_url or "",
                template_text=email_template_text,
                pricing_status=pricing_status,
                missing_prices=missing_prices,
                price_calc_data=price_calc_data,
                raw_pricing_text=raw_pricelist_text or "",
            )
            template_status = str(template_result.get("status") or "template")
            if template_status != "all_prices_found":
                outreach_data = {
                    "clinic_name": name,
                    "subject": template_result.get("subject"),
                    "body": template_result.get("body"),
                    "questions_missing_prices": template_result.get("questions_missing_prices") or missing_prices,
                    "template_preservation_check": template_result.get("template_preservation_check") or {},
                    "status": template_status,
                }
            outreach_evidence_paths = _write_template_outreach_evidence(
                clinic_root=evidence_dirs.clinic_root,
                template_result=template_result,
            )

        if needs_manual_search:
            stats["gemini_skipped_missing_website"] += 1
            if not price_evidence_paths:
                price_evidence_paths = {"status": SKIPPED_MISSING_WEBSITE_STATUS}
            if not outreach_evidence_paths:
                outreach_evidence_paths = {"status": SKIPPED_MISSING_WEBSITE_STATUS}

        if pricing_status == "html_text":
            stats["pricing_html_text"] += 1
        elif pricing_status == "pdf_only":
            stats["pricing_pdf_only"] += 1
        else:
            stats["pricing_none"] += 1

        # Optional Playwright assist mode (autofill only, no submit).
        if playwright_assist and forms and outreach_data and outreach_data.get("body"):
            form_candidate = _choose_form_candidate(forms)
            if form_candidate:
                assist_result = run_playwright_assist(
                    clinic_name=name,
                    form_url=form_candidate,
                    message_body=str(outreach_data.get("body") or ""),
                    evidence_screenshots_dir=evidence_dirs.screenshots_dir,
                    headed=playwright_headed,
                    slowmo_ms=playwright_slowmo_ms,
                )

        captcha_blocked = assist_result is not None and assist_result.status == "captcha_blocked"

        if template_result and str(template_result.get("status") or "") == "all_prices_found":
            queue_status = "skipped_all_prices_found"
            reason = "all_prices_found"
        elif needs_manual_search:
            queue_status = "manual_needed"
            reason = "missing_website"
        elif captcha_blocked:
            queue_status = "manual_needed"
            reason = "captcha_blocked"
        elif outreach_data and emails:
            queue_status = "ready_to_email"
            reason = "email_and_message_ready"
        elif forms and outreach_data and outreach_data.get("body"):
            queue_status = "ready_to_form_assist"
            reason = "form_detected"
        elif forms:
            queue_status = "manual_needed"
            reason = "form_detected_but_no_message"
        else:
            queue_status = "manual_needed"
            reason = "no_contact_method_found"

        stats[queue_status] = stats.get(queue_status, 0) + 1
        stats["processed"] += 1

        outreach_status = "skipped_no_outreach"
        if needs_manual_search:
            outreach_status = SKIPPED_MISSING_WEBSITE_STATUS
        elif template_result and str(template_result.get("status") or "") == "all_prices_found":
            outreach_status = "skipped_all_prices_found"
        elif template_result:
            outreach_status = "template"

        if needs_manual_search:
            gemini_statuses = {
                "price_calc": SKIPPED_MISSING_WEBSITE_STATUS,
                "outreach": outreach_status,
            }
        else:
            gemini_statuses = {
                "price_calc": price_calc_result.status if price_calc_result else "skipped_no_pricing_text",
                "outreach": outreach_status,
            }

        evidence_paths = {
            "clinic_root": str(evidence_dirs.clinic_root),
            "pages_dir": str(evidence_dirs.pages_dir),
            "pdf_dir": str(evidence_dirs.pdf_dir),
            "screenshots_dir": str(evidence_dirs.screenshots_dir),
            "gemini_dir": str(evidence_dirs.gemini_dir),
            "extracted_pricing_path": str(evidence_dirs.extracted_pricing_path)
            if evidence_dirs.extracted_pricing_path.exists()
            else "",
            "gemini_price_calc": price_evidence_paths,
            "outreach_template": outreach_evidence_paths,
        }

        found_urls_pricing = sorted({p.final_url for p in pricing_pages})
        found_urls_contact = sorted({p.final_url for p in contact_pages})

        result_row = {
            "rank": rank,
            "clinic_name": name,
            "place_id": place_id,
            "clinic_slug": clinic_slug,
            "run_id": run_id,
            "run_dir": str(run_dir),
            "quality": row.get("quality"),
            "rating": row.get("rating"),
            "user_rating_count": row.get("user_rating_count"),
            "website_url": website_url or "",
            "needs_manual_search": needs_manual_search,
            "refresh_places_requested": refresh_places_effective,
            "outreach_force": bool(outreach_force),
            "gmail_drafts_enabled": gmail_drafts,
            "gmail_send_enabled": gmail_send,
            "gmail_send_ack": gmail_send_ack,
            "gmail_send_dry_run": gmail_send_dry_run,
            "gmail_daily_limit": gmail_daily_limit,
            "gmail_allow_domains": sorted(gmail_allow_domains_set),
            "crawl": {
                "pages_crawled": len(crawl_result.pages) if crawl_result else 0,
                "visited_urls": crawl_result.visited_urls if crawl_result else [],
                "errors": crawl_errors,
                "cache_hits": cache_hits,
                "live_fetches": live_fetches,
            },
            "discovered": {
                "pricing_pages": found_urls_pricing,
                "contact_pages": found_urls_contact,
                "pricing_pdfs": pdf_urls,
                "pricing_pdf_paths": pdf_paths,
                "emails": emails,
                "forms": forms,
            },
            "pricing": {
                "pricing_status": pricing_status,
                "pricing_text_chars": pricing_text_chars,
                "pricing_text_sufficient": pricing_text_sufficient,
                "missing_prices": missing_prices,
                "price_calc_summary": _summarize_price_calc(price_calc_data),
                "evidence_level_hint": _evidence_level_from_text(raw_pricelist_text or ""),
            },
            "gemini_status": gemini_statuses,
            "gemini": {
                "price_calc": price_calc_data,
                "outreach_message": outreach_data,
            },
            "playwright_assist": {
                "status": assist_result.status if assist_result else "not_requested",
                "form_url": assist_result.form_url if assist_result else "",
                "screenshots": assist_result.screenshot_paths if assist_result else [],
                "notes": assist_result.notes if assist_result else [],
                "error": assist_result.error if assist_result else None,
                "headed": bool(playwright_headed),
                "slowmo_ms": int(playwright_slowmo_ms),
            },
            "suggested_action": {
                "status": queue_status,
                "reason": reason,
                "manual_review_required": True,
            },
            "evidence": evidence_paths,
            "generated_at": _utc_now_iso(),
        }
        results.append(result_row)

        queue_rows.append(
            {
                "clinic_name": name,
                "place_id": place_id,
                "clinic_slug": clinic_slug,
                "run_id": run_id,
                "run_dir": str(run_dir),
                "website_url": website_url or "",
                "status": queue_status,
                "reason": reason,
                "emails": emails,
                "forms": forms,
                "pricing_status": pricing_status,
                "gemini_status": gemini_statuses,
                "manual_review_required": True,
                "gmail_drafts_enabled": gmail_drafts,
                "gmail_send_enabled": gmail_send,
                "gmail_send_ack": gmail_send_ack,
                "gmail_send_dry_run": gmail_send_dry_run,
                "gmail_daily_limit": gmail_daily_limit,
                "gmail_allow_domains": sorted(gmail_allow_domains_set),
                "outreach_force": bool(outreach_force),
                "evidence_dir": str(evidence_dirs.clinic_root),
            }
        )

    gmail_enabled = bool(gmail_drafts or gmail_send)
    if gmail_enabled:
        gmail_stats = _process_gmail_actions(
            results=results,
            queue_rows=queue_rows,
            out_dir=out_dir,
            run_id=run_id,
            gmail_drafts=gmail_drafts,
            gmail_sender_email=gmail_sender_email,
            gmail_max_drafts=gmail_max_drafts,
            gmail_send=gmail_send,
            gmail_send_ack=gmail_send_ack,
            gmail_send_dry_run=gmail_send_dry_run,
            gmail_daily_limit=gmail_daily_limit,
            gmail_allow_domains=gmail_allow_domains_set,
            gmail_send_log_path=gmail_send_log_path,
            gmail_sender=gmail_sender,
        )
        for key, val in gmail_stats.items():
            stats[key] = stats.get(key, 0) + int(val)

    gmail_report_path = out_dir / "outreach_gmail_report.txt"
    if not gmail_enabled or not gmail_report_path.exists():
        gmail_report_path = None

    results_path = out_dir / "outreach_results.json"
    queue_path = out_dir / "outreach_queue.jsonl"
    summary_path = out_dir / "outreach_summary.txt"

    _write_json(results_path, results)
    _write_jsonl(queue_path, queue_rows)

    qa_report_path = _write_qa_report(
        run_dir=out_dir,
        run_id=run_id,
        input_path=input_path,
        results=results,
        gmail_report_path=gmail_report_path,
        preflight_info=preflight_info,
    )

    allow_domains_str = ",".join(sorted(gmail_allow_domains_set))

    summary_lines = [
        "Outreach Summary",
        f"generated_at: {_utc_now_iso()}",
        f"out_parent: {out_parent}",
        f"run_id: {run_id}",
        f"run_dir: {out_dir}",
        f"outreach_input: {input_path}",
        f"qa_report: {qa_report_path}",
        f"gmail_report: {gmail_report_path or ''}",
        f"top_n: {top_n}",
        f"max_pages: {max_pages}",
        f"refresh_web: {refresh_web_effective}",
        f"refresh_places: {refresh_places_effective}",
        f"outreach_force: {bool(outreach_force)}",
        f"playwright_headed: {bool(playwright_headed)}",
        f"playwright_slowmo_ms: {int(playwright_slowmo_ms)}",
        f"gmail_drafts: {gmail_drafts}",
        f"gmail_sender_email: {gmail_sender_email}",
        f"gmail_max_drafts: {gmail_max_drafts}",
        f"gmail_send: {gmail_send}",
        f"gmail_send_ack: {gmail_send_ack}",
        f"gmail_send_dry_run: {gmail_send_dry_run}",
        f"gmail_daily_limit: {gmail_daily_limit}",
        f"gmail_allow_domains: {allow_domains_str}",
        "",
        f"processed: {stats['processed']}",
        f"skipped_do_not_contact: {stats['skipped_do_not_contact']}",
        f"skipped_all_prices_found: {stats['skipped_all_prices_found']}",
        f"needs_manual_search: {stats['needs_manual_search']}",
        f"ready_to_email: {stats['ready_to_email']}",
        f"ready_to_form_assist: {stats['ready_to_form_assist']}",
        f"manual_needed: {stats['manual_needed']}",
        "",
        f"pricing_html_text: {stats['pricing_html_text']}",
        f"pricing_pdf_only: {stats['pricing_pdf_only']}",
        f"pricing_none_or_partial: {stats['pricing_none']}",
        "",
        f"gemini_invalid_json: {stats['gemini_invalid_json']}",
        f"gemini_skipped_no_api_key: {stats['gemini_skipped_no_api_key']}",
        f"gemini_skipped_missing_website: {stats['gemini_skipped_missing_website']}",
        f"gemini_cached_ok: {stats['gemini_cached_ok']}",
        "",
        f"gmail_candidates: {stats['gmail_candidates']}",
        f"gmail_drafts_created: {stats['gmail_drafts_created']}",
        f"gmail_attempted: {stats['gmail_attempted']}",
        f"gmail_sent: {stats['gmail_sent']}",
        f"gmail_blocked: {stats['gmail_blocked']}",
    ]
    atomic_write_text(str(summary_path), "\n".join(summary_lines))

    _write_latest_run_index(
        out_parent=out_parent,
        run_dir=out_dir,
        run_id=run_id,
        stats=stats,
        results_path=results_path,
        queue_path=queue_path,
        summary_path=summary_path,
        qa_report_path=qa_report_path,
        gmail_report_path=gmail_report_path,
    )

    return OutreachRunResult(
        run_id=run_id,
        run_dir=out_dir,
        results_path=results_path,
        queue_path=queue_path,
        summary_path=summary_path,
        qa_report_path=qa_report_path,
        gmail_report_path=gmail_report_path,
        evidence_root=evidence_root,
        processed_count=len(results),
    )
