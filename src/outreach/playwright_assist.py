"""Playwright assist mode.

This module never submits forms. It only attempts to open a contact form page,
autofill obvious fields, take screenshots, and stop.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_MS = 20_000
CAPTCHA_HINTS = ("captcha", "recaptcha", "hcaptcha", "g-recaptcha")


@dataclass(frozen=True)
class AssistResult:
    status: str
    form_url: str
    screenshot_paths: List[str]
    notes: List[str]
    error: Optional[str] = None


def _detect_captcha(page) -> bool:  # pragma: no cover - exercised via integration
    try:
        html = page.content().lower()
    except Exception:
        html = ""
    if any(hint in html for hint in CAPTCHA_HINTS):
        return True
    try:
        if page.locator("iframe[src*='recaptcha'], div.g-recaptcha, iframe[src*='hcaptcha']").count() > 0:
            return True
    except Exception:
        return False
    return False


def _fill_first(page, selectors: List[str], value: str) -> bool:  # pragma: no cover - exercised via integration
    for selector in selectors:
        try:
            loc = page.locator(selector)
            if loc.count() == 0:
                continue
            loc.first.fill(value)
            return True
        except Exception:
            continue
    return False


def run_playwright_assist(
    *,
    clinic_name: str,
    form_url: str,
    message_body: str,
    evidence_screenshots_dir: Path,
    contact_name: str = "Kasper",
    contact_email: str = "kasper@example.com",
    headed: bool = False,
    slowmo_ms: int = 0,
    sync_playwright_factory: Optional[Callable[[], Any]] = None,
) -> AssistResult:
    evidence_screenshots_dir.mkdir(parents=True, exist_ok=True)

    headed = bool(headed)
    slowmo_ms = max(0, int(slowmo_ms))

    if sync_playwright_factory is None:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # pragma: no cover - depends on optional dependency
            return AssistResult(
                status="missing_playwright",
                form_url=form_url,
                screenshot_paths=[],
                notes=["Playwright is not installed; assist mode skipped."],
                error=str(exc),
            )
        sync_playwright_factory = sync_playwright

    screenshots: List[str] = []
    notes: List[str] = []

    try:
        with sync_playwright_factory() as p:  # pragma: no cover - depends on optional dependency
            browser = p.chromium.launch(headless=not headed, slow_mo=slowmo_ms)
            page = browser.new_page()
            page.goto(form_url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)

            if _detect_captcha(page):
                captcha_path = evidence_screenshots_dir / "assist_captcha_detected.png"
                page.screenshot(path=str(captcha_path), full_page=True)
                screenshots.append(str(captcha_path))
                browser.close()
                return AssistResult(
                    status="captcha_blocked",
                    form_url=form_url,
                    screenshot_paths=screenshots,
                    notes=["Captcha detected; stopped before any interaction."],
                )

            filled_name = _fill_first(
                page,
                [
                    "input[name*='name' i]",
                    "input[id*='name' i]",
                    "input[name*='imi' i]",
                    "input[id*='imi' i]",
                    "input[placeholder*='imi' i]",
                    "input[placeholder*='name' i]",
                ],
                contact_name,
            )
            filled_email = _fill_first(
                page,
                [
                    "input[type='email']",
                    "input[name*='mail' i]",
                    "input[id*='mail' i]",
                    "input[placeholder*='mail' i]",
                ],
                contact_email,
            )
            filled_message = _fill_first(
                page,
                [
                    "textarea[name*='msg' i]",
                    "textarea[name*='wiad' i]",
                    "textarea[id*='msg' i]",
                    "textarea[id*='wiad' i]",
                    "textarea",
                    "input[name*='msg' i]",
                ],
                message_body,
            )

            notes.append(f"filled_name={filled_name}")
            notes.append(f"filled_email={filled_email}")
            notes.append(f"filled_message={filled_message}")
            notes.append(f"headed={headed}")
            notes.append(f"slowmo_ms={slowmo_ms}")

            filled_path = evidence_screenshots_dir / "assist_filled_no_submit.png"
            page.screenshot(path=str(filled_path), full_page=True)
            screenshots.append(str(filled_path))
            browser.close()
    except Exception as exc:  # pragma: no cover - depends on optional dependency
        logger.exception("Playwright assist failed for %s", clinic_name)
        return AssistResult(
            status="error",
            form_url=form_url,
            screenshot_paths=screenshots,
            notes=notes,
            error=str(exc),
        )

    notes.append("Stopped before submit by design.")
    return AssistResult(
        status="ok",
        form_url=form_url,
        screenshot_paths=screenshots,
        notes=notes,
    )
