#!/usr/bin/env python3
"""
Retry failed Gemini API calls without re-crawling.

This script scans the evidence folders for failed Gemini attempts (429 errors,
timeouts, etc.) and retries them using the currently configured model.

Features:
- Automatic model rotation on 429 rate limit errors
- Asks for confirmation before switching models
"""
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
except ImportError:
    pass

# Available models in order of preference
AVAILABLE_MODELS = [
    "gemini-3-flash-preview",
    "gemini-2.5-flash",
    "gemini-3-pro-preview",
    "gemini-2.5-pro",
    "gemini-2.0-flash",
]


def get_latest_run_dir() -> Optional[Path]:
    """Get path to latest outreach run directory."""
    pointer = REPO_ROOT / "out" / "outreach" / "latest_run_dir.txt"
    if not pointer.exists():
        return None
    run_dir = Path(pointer.read_text().strip())
    return run_dir if run_dir.exists() else None


def find_failed_gemini_calls(run_dir: Path) -> List[Tuple[Path, Dict[str, Any]]]:
    """Find all evidence folders with failed Gemini attempts."""
    evidence_dir = run_dir / "evidence"
    if not evidence_dir.exists():
        return []
    
    failed = []
    for clinic_dir in evidence_dir.iterdir():
        if not clinic_dir.is_dir():
            continue
        
        gemini_dir = clinic_dir / "gemini"
        if not gemini_dir.exists():
            continue
        
        # Find the latest attempt
        latest_file = gemini_dir / "latest_attempt.txt"
        if not latest_file.exists():
            continue
        
        attempt_name = latest_file.read_text().strip()
        attempt_dir = gemini_dir / attempt_name
        if not attempt_dir.exists():
            continue
        
        # Check the meta file for status
        meta_files = list(attempt_dir.glob("*.meta.json"))
        for meta_file in meta_files:
            try:
                meta = json.loads(meta_file.read_text())
                status = meta.get("status", "")
                error = meta.get("error", "")
                
                # Check for retryable errors
                if status in ("http_error", "timeout", "connection_error", "invalid_json"):
                    failed.append((clinic_dir, {
                        "attempt_dir": attempt_dir,
                        "meta_file": meta_file,
                        "status": status,
                        "error": error,
                        "model": meta.get("model", "unknown"),
                        "prompt_name": meta.get("prompt_name", "unknown"),
                    }))
            except Exception:
                continue
    
    return failed


def retry_gemini_call(
    clinic_dir: Path,
    failure_info: Dict[str, Any],
    gemini_client: Any,
    prompts_dir: Path,
) -> Tuple[bool, str, bool]:
    """
    Retry a single failed Gemini call.
    Returns: (success, message, is_rate_limited)
    """
    attempt_dir = failure_info["attempt_dir"]
    prompt_name = failure_info["prompt_name"]
    
    # Find the raw pricelist text
    raw_file = attempt_dir / "raw_pricelist_text.txt"
    if not raw_file.exists():
        return False, "No raw pricelist text found", False
    
    pricing_text = raw_file.read_text(encoding="utf-8")
    if not pricing_text.strip():
        return False, "Empty pricelist text", False
    
    # Load the prompt template
    prompt_file = prompts_dir / prompt_name
    if not prompt_file.exists():
        return False, f"Prompt file not found: {prompt_name}", False
    
    prompt_template = prompt_file.read_text(encoding="utf-8")
    
    # Build the prompt
    clinic_name = clinic_dir.name.split("-chij")[0].replace("-", " ").title()
    full_prompt = prompt_template.replace("{CLINIC_NAME}", clinic_name)
    full_prompt = full_prompt.replace("{PRICING_TEXT}", pricing_text)
    
    # Call Gemini
    from src.gemini_client import hash_text
    prompt_hash = hash_text(full_prompt)
    
    result = gemini_client.generate_json(
        prompt_name=prompt_name,
        prompt_text=full_prompt,
        prompt_hash=prompt_hash,
    )
    
    # Check for rate limit
    is_rate_limited = result.status == "http_error" and "429" in (result.error or "")
    
    if result.status == "ok":
        # Save the new result
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_attempt_name = f"retry_{timestamp}"
        new_attempt_dir = clinic_dir / "gemini" / new_attempt_name
        new_attempt_dir.mkdir(parents=True, exist_ok=True)
        
        # Save meta
        meta = {
            "prompt_name": prompt_name,
            "prompt_hash": prompt_hash,
            "attempt_name": new_attempt_name,
            "status": result.status,
            "model": result.model,
            "generated_at": datetime.now().isoformat(),
            "retry_of": str(failure_info["attempt_dir"]),
        }
        (new_attempt_dir / f"{prompt_name}.meta.json").write_text(
            json.dumps(meta, indent=2), encoding="utf-8"
        )
        
        # Save raw response
        (new_attempt_dir / f"{prompt_name}.raw.txt").write_text(
            result.raw_text, encoding="utf-8"
        )
        
        # Save parsed data
        if result.data:
            (new_attempt_dir / f"{prompt_name}.parsed.json").write_text(
                json.dumps(result.data, indent=2), encoding="utf-8"
            )
        
        # Copy the raw pricelist
        (new_attempt_dir / "raw_pricelist_text.txt").write_text(
            pricing_text, encoding="utf-8"
        )
        
        # Update latest pointer
        (clinic_dir / "gemini" / "latest_attempt.txt").write_text(new_attempt_name)
        
        return True, f"Success with {result.model}", False
    else:
        return False, f"{result.status}: {result.error}", is_rate_limited


def update_results_file(run_dir: Path, retried_clinics: Dict[str, Dict]) -> int:
    """Update outreach_results.json with new Gemini data."""
    results_file = run_dir / "outreach_results.json"
    if not results_file.exists():
        return 0
    
    results = json.loads(results_file.read_text(encoding="utf-8"))
    updated = 0
    
    for row in results:
        clinic_name = row.get("clinic_name", "")
        if clinic_name not in retried_clinics:
            continue
        
        retry_data = retried_clinics[clinic_name]
        if not retry_data.get("success"):
            continue
        
        # Load the new parsed data
        parsed_file = retry_data.get("parsed_file")
        if parsed_file and Path(parsed_file).exists():
            try:
                new_data = json.loads(Path(parsed_file).read_text())
                if "gemini" not in row:
                    row["gemini"] = {}
                row["gemini"]["price_calc"] = new_data
                updated += 1
            except Exception:
                pass
    
    if updated:
        results_file.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    
    return updated


def get_next_model(current_model: str, tried_models: set) -> Optional[str]:
    """Get the next available model that hasn't been tried."""
    for model in AVAILABLE_MODELS:
        if model != current_model and model not in tried_models:
            return model
    return None


def main():
    print("=" * 60)
    print("Gemini Retry Tool (with model rotation)")
    print("=" * 60)
    
    # Check for API key
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        print("❌ No GEMINI_API_KEY found in environment")
        return 1
    
    current_model = os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")
    tried_models = set()
    
    print(f"Starting with model: {current_model}")
    print(f"Available models: {', '.join(AVAILABLE_MODELS)}")
    
    # Get latest run
    run_dir = get_latest_run_dir()
    if not run_dir:
        print("❌ No outreach run found")
        return 1
    
    print(f"Run directory: {run_dir}")
    
    # Find failed calls
    failed = find_failed_gemini_calls(run_dir)
    if not failed:
        print("✓ No failed Gemini calls found")
        return 0
    
    print(f"\nFound {len(failed)} failed Gemini calls:")
    for clinic_dir, info in failed:
        clinic_name = clinic_dir.name.split("-chij")[0].replace("-", " ").title()
        print(f"  - {clinic_name}: {info['error']}")
    
    # Confirm
    print(f"\nRetry all {len(failed)} failed calls? [y/N] ", end="")
    response = input().strip().lower()
    if response != "y":
        print("Cancelled")
        return 0
    
    # Initialize Gemini client
    from src.gemini_client import GeminiClient
    gemini = GeminiClient(api_key=api_key, model=current_model)
    prompts_dir = REPO_ROOT / "prompts"
    
    # Retry each
    print("\nRetrying...")
    retried = {}
    success_count = 0
    pending = list(failed)
    i = 0
    
    while i < len(pending):
        clinic_dir, info = pending[i]
        clinic_name = clinic_dir.name.split("-chij")[0].replace("-", " ").title()
        print(f"[{i+1}/{len(pending)}] {clinic_name}... ", end="", flush=True)
        
        success, message, is_rate_limited = retry_gemini_call(
            clinic_dir, info, gemini, prompts_dir
        )
        
        if success:
            print(f"✓ {message}")
            success_count += 1
            
            # Find the parsed file
            latest = (clinic_dir / "gemini" / "latest_attempt.txt").read_text().strip()
            parsed = clinic_dir / "gemini" / latest / f"{info['prompt_name']}.parsed.json"
            
            retried[clinic_name] = {
                "success": True,
                "parsed_file": str(parsed) if parsed.exists() else None,
            }
            
            # Small delay between successful calls
            time.sleep(0.5)
            i += 1
            
        elif is_rate_limited:
            print(f"⚠️ Rate limited ({current_model})")
            
            # Immediately ask to switch models
            tried_models.add(current_model)
            next_model = get_next_model(current_model, tried_models)
            
            if next_model:
                remaining = len(pending) - i
                print(f"\n   {remaining} calls remaining")
                print(f"\nSwitch to {next_model}? [y/N] ", end="")
                switch = input().strip().lower()
                
                if switch == "y":
                    current_model = next_model
                    gemini = GeminiClient(api_key=api_key, model=current_model)
                    print(f"✓ Switched to {current_model}\n")
                    time.sleep(1)  # Brief pause before continuing
                    # Don't increment i - retry this same item with new model
                else:
                    print("Stopping here.")
                    break
            else:
                print(f"\n❌ All models exhausted. {len(pending) - i} calls still pending.")
                break
        else:
            print(f"✗ {message}")
            retried[clinic_name] = {"success": False, "error": message}
            i += 1
    
    print(f"\n{'=' * 60}")
    print(f"Results: {success_count}/{len(failed)} successful")
    
    # Update results file
    if success_count > 0:
        print("\nUpdating outreach_results.json...")
        updated = update_results_file(run_dir, retried)
        print(f"Updated {updated} entries in results file")
        
        print("\n💡 Run `python3 run.py --generate-price-list` to refresh the price comparison")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
