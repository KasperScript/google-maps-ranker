#!/usr/bin/env python3
"""
Send Gmail drafts gradually with random delays between sends.
This helps avoid spam detection and gives a more natural sending pattern.
"""
import json
import random
import sys
import time
from datetime import datetime
from pathlib import Path

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv
load_dotenv()

from src.gmail_sender import GmailSender

LABEL_NAME = "OrthoRanker"
MIN_DELAY_SECONDS = 5
MAX_DELAY_SECONDS = 10
LOG_FILE = REPO_ROOT / "out" / "send_log.jsonl"


def log_send(entry: dict) -> None:
    """Append a send log entry."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def main():
    print("=" * 50)
    print("Gradual Draft Sender")
    print("=" * 50)
    
    try:
        sender = GmailSender()
        my_email = sender.get_profile_email()
        print(f"Authenticated as: {my_email}")
    except Exception as e:
        print(f"Failed to authenticate: {e}")
        return 1

    # Get all drafts
    service = sender.service
    try:
        drafts_response = service.users().drafts().list(userId="me").execute()
        drafts = drafts_response.get("drafts", [])
    except Exception as e:
        print(f"Failed to list drafts: {e}")
        return 1

    if not drafts:
        print("No drafts found.")
        return 0

    print(f"Found {len(drafts)} total drafts.")
    
    # Filter for OrthoRanker label (check each draft's message)
    ortho_drafts = []
    for draft in drafts:
        try:
            draft_data = service.users().drafts().get(userId="me", id=draft["id"]).execute()
            msg = draft_data.get("message", {})
            labels = msg.get("labelIds", [])
            
            # Get subject for display
            headers = msg.get("payload", {}).get("headers", [])
            subject = next((h["value"] for h in headers if h["name"].lower() == "subject"), "No Subject")
            to_email = next((h["value"] for h in headers if h["name"].lower() == "to"), "Unknown")
            
            # Check if it has OrthoRanker label or was created by our system
            # For simplicity, include all drafts but let user confirm
            ortho_drafts.append({
                "id": draft["id"],
                "subject": subject,
                "to": to_email,
            })
        except Exception as e:
            print(f"  Error reading draft {draft['id']}: {e}")
            continue

    if not ortho_drafts:
        print("No drafts to send.")
        return 0

    print(f"\nReady to send {len(ortho_drafts)} drafts:")
    for i, d in enumerate(ortho_drafts, 1):
        print(f"  {i}. To: {d['to'][:50]}... | Subject: {d['subject'][:40]}...")
    
    print(f"\nThis will send emails with {MIN_DELAY_SECONDS}-{MAX_DELAY_SECONDS}s random delays.")
    confirm = input("Type 'SEND' to proceed: ").strip()
    
    if confirm != "SEND":
        print("Aborted.")
        return 0

    sent_count = 0
    errors = []
    
    for i, draft in enumerate(ortho_drafts, 1):
        print(f"\n[{i}/{len(ortho_drafts)}] Sending to: {draft['to']}")
        
        try:
            # Send the draft
            result = service.users().drafts().send(
                userId="me",
                body={"id": draft["id"]}
            ).execute()
            
            message_id = result.get("id", "")
            thread_id = result.get("threadId", "")
            
            print(f"  ✓ Sent! Message ID: {message_id[:20]}...")
            
            log_send({
                "timestamp": datetime.now().isoformat(),
                "to": draft["to"],
                "subject": draft["subject"],
                "message_id": message_id,
                "thread_id": thread_id,
                "status": "sent",
            })
            
            sent_count += 1
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            errors.append({"draft": draft, "error": str(e)})
            log_send({
                "timestamp": datetime.now().isoformat(),
                "to": draft["to"],
                "subject": draft["subject"],
                "status": "error",
                "error": str(e),
            })

        # Delay before next send (except for last one)
        if i < len(ortho_drafts):
            delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
            print(f"  Waiting {delay:.1f}s before next send...")
            time.sleep(delay)

    print("\n" + "=" * 50)
    print(f"Done! Sent: {sent_count}, Errors: {len(errors)}")
    print(f"Log saved to: {LOG_FILE}")
    
    if errors:
        print("\nErrors:")
        for err in errors:
            print(f"  - {err['draft']['to']}: {err['error']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
