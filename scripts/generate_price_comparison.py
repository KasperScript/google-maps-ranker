#!/usr/bin/env python3
"""
Generate a comprehensive price comparison table with:
- Rank and quality score
- A, B, C cost scenarios
- Travel times from ALK, Galeria Północna, Dworzec Centralny
- Manual check needed section
- Archiving of old files on new run
"""
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# Paths
DEFAULT_OUTPUT = REPO_ROOT / "out" / "price_comparison.md"
UPDATES_LOG = REPO_ROOT / "out" / "price_updates.md"
ARCHIVE_DIR = REPO_ROOT / "out" / "archive"

# Hub names for display
HUB_DISPLAY_NAMES = {
    "alk": "ALK",
    "centralny": "Centralny",
    "galeria_polnocna": "Galeria Pn.",
}


def get_latest_results_path() -> Optional[Path]:
    """Get path to latest outreach results."""
    pointer = REPO_ROOT / "out" / "outreach" / "latest_run_dir.txt"
    if not pointer.exists():
        return None
    run_dir = Path(pointer.read_text().strip())
    results = run_dir / "outreach_results.json"
    return results if results.exists() else None


def archive_old_files() -> List[str]:
    """Archive existing price files before generating new ones."""
    archived = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    
    for path in [DEFAULT_OUTPUT, UPDATES_LOG]:
        if path.exists():
            archive_name = f"{path.stem}_{timestamp}{path.suffix}"
            dest = ARCHIVE_DIR / archive_name
            shutil.copy2(path, dest)
            archived.append(str(dest))
    
    return archived


def format_travel_time(minutes: Optional[float]) -> str:
    """Format travel time as string."""
    if minutes is None:
        return "—"
    return f"{int(minutes)}'"


def generate_comparison(results: List[Dict[str, Any]]) -> str:
    """Generate markdown comparison table."""
    lines = [
        "# Orthodontic Price Comparison",
        "",
        f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
        "",
    ]
    
    # Categorize clinics by status
    ready_to_email = []
    manual_needed = []
    form_assist = []
    skipped = []
    
    for row in results:
        action = (row.get("suggested_action") or {}).get("status", "")
        if action == "ready_to_email":
            ready_to_email.append(row)
        elif action == "manual_needed":
            manual_needed.append(row)
        elif action == "ready_to_form_assist":
            form_assist.append(row)
        else:
            skipped.append(row)
    
    # Summary section
    lines.append("## Quick Summary")
    lines.append("")
    lines.append("| Status | Count |")
    lines.append("|--------|-------|")
    lines.append(f"| ✅ Ready to Email | {len(ready_to_email)} |")
    lines.append(f"| 📝 Form Assist Needed | {len(form_assist)} |")
    lines.append(f"| ⚠️ Manual Check Needed | {len(manual_needed)} |")
    lines.append(f"| ⏭️ Skipped | {len(skipped)} |")
    lines.append(f"| **Total** | **{len(results)}** |")
    lines.append("")
    
    # ==================== COST COMPARISON TABLE ====================
    lines.append("---")
    lines.append("")
    lines.append("## 💰 Cost Comparison Table")
    lines.append("")
    lines.append("| # | Clinic | Scenario A | Scenario B | Scenario C | ALK | Centralny | Galeria |")
    lines.append("|---|--------|------------|------------|------------|-----|-----------|---------|")
    
    # Sort by rank
    sorted_results = sorted(results, key=lambda x: x.get("rank", 999))
    
    for row in sorted_results:
        rank = row.get("rank", "—")
        name = (row.get("clinic_name") or "Unknown")[:30]
        
        # Extract pricing variants
        gemini = row.get("gemini") or {}
        price_calc = gemini.get("price_calc") or {}
        variants = price_calc.get("variants") or {}
        
        # Get totals for each scenario
        total_a = variants.get("A", {}).get("total")
        total_b = variants.get("B", {}).get("total")
        total_c = variants.get("C", {}).get("total")
        
        price_a = f"{total_a:,} zł" if total_a else "—"
        price_b = f"{total_b:,} zł" if total_b else "—"
        price_c = f"{total_c:,} zł" if total_c else "—"
        
        # Get travel times from transit_times field (from calculate_transit_times.py)
        travel_times = row.get("transit_times") or {}
        
        # Fallback: try quality data or other fields
        if not travel_times:
            quality = row.get("quality") or {}
            for hub_key in ["alk", "centralny", "galeria_polnocna"]:
                time_key = f"transit_time_{hub_key}"
                if time_key in quality:
                    travel_times[hub_key] = quality[time_key]
                elif hub_key in quality:
                    travel_times[hub_key] = quality[hub_key]
        
        time_alk = format_travel_time(travel_times.get("alk"))
        time_cent = format_travel_time(travel_times.get("centralny"))
        time_gal = format_travel_time(travel_times.get("galeria_polnocna"))
        
        lines.append(f"| {rank} | {name} | {price_a} | {price_b} | {price_c} | {time_alk} | {time_cent} | {time_gal} |")
    
    lines.append("")
    lines.append("*Scenario A = optimistic | B = moderate | C = conservative*")
    lines.append("")
    
    # ==================== MANUAL CHECK NEEDED ====================
    if manual_needed:
        lines.append("---")
        lines.append("")
        lines.append("## ⚠️ Manual Check Needed")
        lines.append("")
        lines.append("These clinics need manual outreach (no email found):")
        lines.append("")
        lines.append("| Clinic | Website | Reason |")
        lines.append("|--------|---------|--------|")
        for row in manual_needed:
            name = row.get("clinic_name", "Unknown")[:35]
            website = row.get("website_url", "—")
            if website and website != "—":
                domain = website.replace("https://", "").replace("http://", "").split("/")[0][:25]
                website_link = f"[{domain}]({website})"
            else:
                website_link = "—"
            reason = (row.get("suggested_action") or {}).get("reason", "no_contact_method")
            lines.append(f"| {name} | {website_link} | {reason} |")
        lines.append("")
    
    # ==================== FORM ASSIST ====================
    if form_assist:
        lines.append("---")
        lines.append("")
        lines.append("## 📝 Form Assist Needed")
        lines.append("")
        lines.append("These clinics have contact forms but no email:")
        lines.append("")
        for row in form_assist:
            name = row.get("clinic_name", "Unknown")
            website = row.get("website_url", "")
            if website:
                lines.append(f"- **{name}** - [{website}]({website})")
            else:
                lines.append(f"- **{name}**")
        lines.append("")
    
    # ==================== DETAILED CLINIC INFO ====================
    lines.append("---")
    lines.append("")
    lines.append("## 📋 Clinic Details")
    lines.append("")
    lines.append("**Legend:** 🌐 = from website | 📧 = from email response | — = not found")
    lines.append("")
    
    for row in sorted_results:
        rank = row.get("rank", "—")
        name = row.get("clinic_name") or row.get("name") or "Unknown"
        website = row.get("website_url") or ""
        
        # Extract pricing data
        gemini = row.get("gemini") or {}
        price_calc = gemini.get("price_calc") or {}
        variants = price_calc.get("variants") or {}
        variant_a = variants.get("A") or {}
        breakdown = variant_a.get("breakdown") or {}
        
        # Get individual prices
        total = variant_a.get("total")
        confidence = variant_a.get("confidence", "")
        missing_count = variant_a.get("missing_items_count", 0)
        fallback_count = variant_a.get("fallback_items_count", 0)
        
        # Status
        action = (row.get("suggested_action") or {}).get("status", "")
        
        # Get email if found
        discovered = row.get("discovered") or {}
        emails = discovered.get("emails") or []
        email = emails[0] if emails else "—"
        
        lines.append(f"### #{rank} {name}")
        lines.append("")
        if website:
            domain = website.replace("https://", "").replace("http://", "").split("/")[0]
            lines.append(f"🔗 [{domain}]({website})")
        lines.append(f"📧 {email}")
        lines.append(f"📊 Status: `{action}`")
        lines.append("")
        
        # Show all scenarios
        if variants:
            lines.append("| Scenario | Total | Confidence | Missing |")
            lines.append("|----------|-------|------------|---------|")
            for scenario in ["A", "B", "C"]:
                v = variants.get(scenario, {})
                t = v.get("total")
                c = v.get("confidence", "—")
                m = v.get("missing_items_count", 0)
                if t:
                    lines.append(f"| {scenario} | {t:,} zł | {c} | {m} items |")
            lines.append("")
        elif total:
            lines.append(f"**Estimated Total: {total:,} zł** (confidence: {confidence})")
            if missing_count:
                lines.append(f"⚠️ {missing_count} prices missing, {fallback_count} using fallback values")
            lines.append("")
        else:
            lines.append("**Pricing data not available**")
            lines.append("")
        
        # Show breakdown if available
        if breakdown:
            lines.append("<details>")
            lines.append("<summary>Price Breakdown</summary>")
            lines.append("")
            lines.append("| Item | Price |")
            lines.append("|------|-------|")
            for category, items in breakdown.items():
                if isinstance(items, dict):
                    for item_name, price in items.items():
                        if isinstance(price, (int, float)):
                            lines.append(f"| {item_name} | {int(price)} zł |")
            lines.append("")
            lines.append("</details>")
        
        lines.append("")
        lines.append("---")
        lines.append("")
    
    return "\n".join(lines)


def main():
    results_path = get_latest_results_path()
    
    if not results_path:
        print("No outreach results found. Run the outreach pipeline first.")
        return 1
    
    print(f"Reading results from: {results_path}")
    data = json.loads(results_path.read_text(encoding="utf-8"))
    
    if not data:
        print("No data in results file.")
        return 1
    
    print(f"Processing {len(data)} clinics...")
    
    # Archive old files
    archived = archive_old_files()
    for path in archived:
        print(f"📦 Archived: {path}")
    
    # Generate markdown comparison
    md_content = generate_comparison(data)
    
    # Write markdown
    DEFAULT_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_OUTPUT.write_text(md_content, encoding="utf-8")
    print(f"✓ Price comparison saved to: {DEFAULT_OUTPUT}")
    
    # Generate CSV for Excel
    csv_path = DEFAULT_OUTPUT.with_suffix(".csv")
    csv_lines = ["Rank,Clinic,Scenario A,Scenario B,Scenario C,ALK,Centralny,Galeria"]
    
    sorted_data = sorted(data, key=lambda x: x.get("rank", 999))
    for row in sorted_data:
        rank = row.get("rank", "")
        name = (row.get("clinic_name") or "Unknown").replace(",", " ")
        
        gemini = row.get("gemini") or {}
        price_calc = gemini.get("price_calc") or {}
        variants = price_calc.get("variants") or {}
        
        total_a = variants.get("A", {}).get("total", "")
        total_b = variants.get("B", {}).get("total", "")
        total_c = variants.get("C", {}).get("total", "")
        
        times = row.get("transit_times") or {}
        t_alk = times.get("alk", "")
        t_cen = times.get("centralny", "")
        t_gal = times.get("galeria_polnocna", "")
        
        csv_lines.append(f"{rank},{name},{total_a},{total_b},{total_c},{t_alk},{t_cen},{t_gal}")
    
    csv_path.write_text("\n".join(csv_lines), encoding="utf-8")
    print(f"✓ CSV export saved to: {csv_path}")
    
    # Initialize updates log if not exists
    if not UPDATES_LOG.exists():
        UPDATES_LOG.write_text(
            "# Price Updates Log\n\n"
            "Tracks updates from email responses.\n\n"
            "---\n\n"
            "*No updates yet.*\n",
            encoding="utf-8"
        )
        print(f"✓ Updates log created: {UPDATES_LOG}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
