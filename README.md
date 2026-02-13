# Google Maps Place Ranker

Find and rank the best places anywhere using Google Maps data. Searches for places by customizable queries, scores them using a Bayesian quality model trained on ratings and review counts, and optionally factors in public-transit travel time.

## Quick Start

### 1. Get API Keys

You need two API keys from Google Cloud:

- **Google Maps API Key** — enable [Places API (New)](https://developers.google.com/maps/documentation/places/web-service) and [Routes API](https://developers.google.com/maps/documentation/routes)
- **Gemini API Key** — from [Google AI Studio](https://aistudio.google.com/apikey) (for search term generation)

### 2. Run the Setup Wizard

The setup wizard walks you through configuration:

```bash
pip install -r requirements.txt
python setup_server.py
```

Open [http://localhost:8000](http://localhost:8000) and follow the steps:

1. **API Keys** — enter your Google Maps and Gemini keys
2. **What to Search** — describe what you're looking for; Gemini generates optimized search queries
3. **Location** — click on the map, search for a place, or enter coordinates
4. **Distance** — set the maximum search radius

The wizard saves your configuration to `search_config.json` and `.env`.

### 3. Run the Ranker

```bash
# Radius scan mode (recommended for first run)
python run.py --radius-scan --center-lat 52.23 --center-lon 21.00 --radius-km 15

# Or use the config from the wizard
python run.py --radius-scan --center-lat <lat> --center-lon <lon>

# Dry run (small request caps, no Routes calls)
python run.py --dry-run
```

Results are written to `out/`:
- `out/results.csv` / `out/results.json` — ranked places
- `out/summary.txt` — run summary
- `out/rejections.csv` — filtered-out places with reasons

## Manual Configuration

Instead of the wizard, you can create `search_config.json` directly:

```json
{
  "description": "Best barbershops in Kraków",
  "center": { "lat": 50.0647, "lon": 19.945, "name": "Kraków" },
  "max_distance_km": 15,
  "queries": {
    "primary": ["barbershop", "barber", "men's haircut"],
    "secondary": ["hair salon"]
  },
  "type_filters": ["hair_care", "beauty_salon"],
  "domain_reject_substrings": ["pet", "animal", "vet"],
  "min_reviews": 50
}
```

And set your API keys in `.env`:

```
GOOGLE_MAPS_API_KEY=your_key_here
GEMINI_API_KEY=your_key_here
```

## CLI Reference

| Flag | Description |
|------|-------------|
| `--radius-scan` | Exhaustive grid scan mode |
| `--center-lat` / `--center-lon` | Search center coordinates |
| `--radius-km` | Search radius in kilometers |
| `--queries` | Comma-separated search queries |
| `--types` | Comma-separated place type filters |
| `--top N` | Top N results by quality |
| `--max-places N` | Places API request budget |
| `--max-routes N` | Routes API request budget |
| `--dry-run` | Tiny request caps for testing |
| `--no-cache` | Skip SQLite cache |
| `--preflight` | Offline config checks |
| `--preflight-online` | Config checks + one API call |
| `--list-mode` | List all candidates (no Routes) |
| `--coverage-mode` | `off`, `light`, or `full` |
| `--out DIR` | Output directory (default: `out`) |

## How It Works

1. **Harvest** — searches Google Maps with multiple queries and type filters across a grid of points
2. **Filter** — removes duplicates, non-operational businesses, low-review places, and out-of-range results
3. **Score** — combines Bayesian average and Wilson lower bound into a quality score
4. **Rank** — optionally adds transit-time scoring via the Routes API
5. **Output** — writes ranked results to CSV and JSON

## Cost Control

The pipeline minimizes API costs via:
- Minimal field masks on all API calls
- SQLite caching (no repeated calls for the same query)
- Strict per-run request budgets
- Staged pipeline (only top candidates get Routes calls)

## Tests

```bash
pip install pytest
pytest tests/ -v
```

## License

MIT
