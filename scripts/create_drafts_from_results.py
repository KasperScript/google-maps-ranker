#!/usr/bin/env python3
"""
Standalone script to create Gmail drafts from existing outreach results.
Useful when the pipeline finishes analysis but fails to create drafts (e.g. auth error).
"""
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

# Add repo root to path to import src
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv
from src.gmail_sender import GmailSender

# Load env before using GmailSender config
load_dotenv()

INPUT_JSON = REPO_ROOT / "out/outreach/20260128_162018/outreach_results.json"
MAX_DRAFTS = 50

def get_recipient(row: Dict[str, Any]) -> str:
    # Try different fields where email might be stored
    discovered = row.get("discovered") or {}
    emails = discovered.get("emails") or []
    if emails:
        return emails[0]
    # Fallback to queue row structure if present
    if "email" in row:
        return row["email"]
    return ""


def clean_text(text: str) -> str:
    """Clean up subject/body text by removing artifacts."""
    if not text:
        return ""
    text = text.strip()
    # Remove "body:" or "Body:" prefix
    for prefix in ("body:", "Body:", "subject:", "Subject:", "temat:", "Temat:"):
        if text.lower().startswith(prefix.lower()):
            text = text[len(prefix):].strip()
    # Replace curly/smart quotes with standard ASCII quotes
    text = text.replace("\u201c", '"').replace("\u201d", '"')
    text = text.replace("\u2018", "'").replace("\u2019", "'")
    # Remove wrapping quotes if present
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        text = text[1:-1]
    return text

def main():
    if not INPUT_JSON.exists():
        print(f"Error: Input file not found at {INPUT_JSON}")
        return 1

    print(f"Reading results from: {INPUT_JSON}")
    data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    
    # Handle both list of dicts and dict with 'results' key
    rows = []
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = data.get("results") or data.get("clinics") or []

    print(f"Found {len(rows)} rows total.")

    # Initialize Gmail Sender
    try:
        sender = GmailSender()
        my_email = sender.get_profile_email()
        print(f"Authenticated as: {my_email}")
    except Exception as e:
        print(f"Failed to authenticate Gmail: {e}")
        print("Did you delete the old token.json and re-authenticate?")
        return 1

    drafts_created = 0
    errors = 0

    for row in rows:
        if drafts_created >= MAX_DRAFTS:
            print(f"Hit max drafts limit ({MAX_DRAFTS}). Stopping.")
            break

        name = row.get("clinic_name") or row.get("name") or "Unknown Clinic"
        
        # Check if it was ready to email
        action = row.get("suggested_action") or {}
        status = action.get("status")
        
        # Also check row-level status from the report logic
        if status != "ready_to_email":
            # Some might have 'template' but missed 'ready_to_email' in one field 
            # but were flagged valid elsewhere. Let's trust 'suggested_action.status'.
            continue

        gemini = row.get("gemini") or {}
        outreach_msg = gemini.get("outreach_message") or {}
        
        subject = clean_text(outreach_msg.get("subject") or "")
        body = clean_text(outreach_msg.get("body") or "")
        
        if not subject or not body:
            print(f"Skipping {name}: Missing subject/body.")
            continue

        to_email = get_recipient(row)
        if not to_email:
            print(f"Skipping {name}: No recipient email found.")
            continue

        print(f"Creating draft for {name} ({to_email})...")
        
        res = sender.create_draft(
            to_email=to_email,
            subject=subject,
            body=body,
            sender_email=my_email,
            label_name="OrthoRanker"
        )
        
        if res.get("status") == "drafted":
            print(f"  -> Draft ID: {res.get('draft_id')}")
            drafts_created += 1
        else:
            print(f"  -> Error: {res.get('error') or res}")
            errors += 1

    print("-" * 30)
    print(f"Done. Created {drafts_created} drafts. Errors: {errors}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
