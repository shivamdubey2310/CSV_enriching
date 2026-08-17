# Quickstart & Run Guide

Fast, copy-pasteable steps to run the enrichment pipeline end-to-end.

---

## Prerequisites Checklist

- Linux / Unix environment
- Docker and Docker Compose installed and running
- Python 3.10+ available as `python3`

---

## Step 1: Environment Setup

```bash
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

---

## Step 2: Start Background Search Engine

The pipeline requires a local SearXNG container for LinkedIn recovery and domain discovery.

```bash
docker compose up -d
```

Verify the container is responding:

```bash
curl -s "http://127.0.0.1:8080/search?q=test&format=json" | head -c 200
```

A JSON response containing a `results` array confirms readiness.

---

## Step 3: Place Input Files

Drop your `.csv` or `.tsv` files into `data/raw/`. The pipeline auto-detects tab vs. comma delimiters by inspecting the first line of each file — no flags or configuration needed.

Column names are normalized to lowercase automatically. Recognized fields: `name`, `email`, `company`, `linkedin_url`, `linkedin_normalized_url`, plus any additional columns which are passed through to the output unchanged.

---

## Step 4: Execute the Pipeline

```bash
python3 run.py -i "data/raw/*.csv" -o data/processed/master_enriched_final.csv
```

### Optional Flags

| Flag | Long form | Default | Description |
|------|-----------|---------|-------------|
| `-i` | `--input` | `data/raw/*.csv` | Input file path or wildcard |
| `-o` | `--output` | `data/processed/master_enriched_final.csv` | Output file path |
| `-b` | `--batch-size` | `15` | Concurrent rows per batch (range: 1–50) |

Output directories are created automatically if they do not exist.

---

## Step 5: Monitor Progress

```bash
tail -f logs/master_enrichment.log
```

Normal log lines look like this:

```
[1/1500] John Smith (Acme Corp) -> Email: john.smith@acme.com [MX_VERIFIED_PATTERN] | LinkedIn: https://www.linkedin.com/in/john-smith [LIVE_PROFILE_CONFIRMED]
[SEARXNG HIT] Jane Doe (Beta Ltd) -> https://www.linkedin.com/in/jane-doe
[VALID DOMAIN FOUND] Gamma Inc -> gamma.example.com
[SCRAPED TEAM HIT] Bob Lee (Delta LLC) -> bob.lee@delta.example.com
```

Status codes you will see:

- `[LIVE_PROFILE_CONFIRMED]` — Input LinkedIn URL probed live via OpenGraph
- `[SEARXNG_VERIFIED_LIVE]` — Profile recovered via SearXNG search
- `[LINKEDIN_UNRESOLVED]` — No valid profile found
- `[MX_VERIFIED_PATTERN]` — Domain has MX records; email set to highest-probability permutation
- `[SCRAPED_TEAM_MATCH]` — Email found on corporate website matching the subject's name
- `[SELF_EMPLOYED_NO_DOMAIN]` — Company field indicates freelance / self-employed; email skipped
- `[UNRESOLVED]` — No domain or MX record found

---

## Step 6: Export Clean Results

Run this after the pipeline completes to filter to rows with verified emails and/or confirmed LinkedIn profiles:

```bash
python3 -c "
import pandas as pd
df = pd.read_csv('data/processed/master_enriched_final.csv')
clean = df[
    (df['verification_status'].isin(['MX_VERIFIED_PATTERN', 'SCRAPED_TEAM_MATCH'])) |
    (df['linkedin_resolution_status'].isin(['LIVE_PROFILE_CONFIRMED', 'SEARXNG_VERIFIED_LIVE']))
]
clean.to_csv('data/processed/clean_verified_output.csv', index=False)
print(f'Exported {len(clean)} of {len(df)} rows')
"
```

---

## Operations & Tips

### Pause and Resume

Press `Ctrl+C` to stop the pipeline at any time. Rerun the same command to resume — already-processed rows are automatically detected and skipped via a composite key (`row_number|name|email`).

```bash
python3 run.py -i "data/raw/*.csv" -o data/processed/master_enriched_final.csv
```

### Reset Cache

To discard all cached results and reprocess from scratch:

```bash
rm -rf .cache/*
```

Then rerun the pipeline command.

### Restart Search Container

If SearXNG stops responding or returns errors:

```bash
docker restart searxng_local
```

Wait a few seconds, then verify with the curl command from Step 2 before re-running the pipeline.
