# Identity Resolution & Data Enrichment Pipeline

## Technical Operations Manual

> **Repository:** `CSV_enriching`
> **Runtime Environment:** Linux / Unix
> **Primary Entry Point:** `run.py` → `src/pipeline.py`

---

## 1. System Overview

This is a high-throughput asynchronous Python pipeline engineered to clean, cross-verify, and recover professional identity data across two independent dimensions: LinkedIn profile URLs and corporate email addresses.

The system operates as a two-phase enrichment engine executed per-row:

- **Phase 1 — Profile Validation & Recovery:** Validates existing LinkedIn URLs via live OpenGraph HTTP probing; recovers unresolvable profiles through a multi-engine SearXNG waterfall search.
- **Phase 2 — Domain & Mailbox Pattern Recovery:** Discovers corporate email domains, validates MX records via asynchronous non-blocking DNS, crawls corporate websites for scraped team matches, and generates deterministic email permutations ordered by statistical frequency.

The pipeline supports concurrent batch processing with row-level checkpoint/resume semantics, automatic CSV/TSV delimiter detection, and self-employed / freelance classification in 20+ languages.

---

## 2. Standardized Directory Layout

```
.
├── .cache/                         # Local runtime caches (gitignored)
│   ├── cache.db
│   └── linkedin_cache.db
├── data/
│   ├── raw/                        # Input CSV / TSV datasets
│   └── processed/                  # Enriched output files
├── logs/
│   └── master_enrichment.log       # Rotating execution log
├── src/
│   ├── __init__.py
│   └── pipeline.py                 # Core async engine and all verification logic
├── docker-compose.yml              # Local SearXNG container definition
├── requirements.txt                # Python dependency lock
├── run.py                          # CLI entry point (thin wrapper)
├── .env                            # API key configuration (gitignored)
└── .gitignore
```

---

## 3. System Architecture & Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        INPUT LAYER                                 │
│  data/raw/*.csv  (single file, wildcard, or comma-separated list)  │
│  Auto-detects delimiter (CSV vs TSV). Normalizes column headers.   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              CHECKPOINT / RESUME GATE                              │
│  Reads existing output file. Builds composite key set:             │
│  key = row_number | name | email                                   │
│  Filters already-processed rows from execution queue.              │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              ASYNC BATCH PROCESSING LOOP                           │
│  Concurrency controlled by -b / --batch-size (default: 15).       │
│  Single shared aiohttp session (curl_cffi Chrome impersonation).   │
│  Semaphores: probe_sem=8 (HTTP), searxng_sem=2 (search).          │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                ┌──────────────┴──────────────┐
                ▼                             ▼
  ┌────────────────────────┐   ┌─────────────────────────────────────┐
  │   PHASE 1              │   │   PHASE 2                           │
  │   LinkedIn Resolution  │   │   Email Recovery                     │
  │                        │   │                                     │
  │  1a. OpenGraph probe   │   │  2a. Original domain MX check        │
  │      (HTTP 200/404     │   │      (async DNS over 1.1.1.1,        │
  │       + OG title       │   │       8.8.8.8, 9.9.9.9)             │
  │       parsing)         │   │                                     │
  │                        │   │  2b. Domain discovery fallback:      │
  │  1b. SearXNG           │   │      - Clearbit autocomplete API     │
  │      multi-engine      │   │      - SearXNG "official website"    │
  │      waterfall         │   │        query (DDG, Brave, Yahoo,      │
  │      (DDG, Brave,      │   │        Qwant)                        │
  │       Yahoo, Qwant,    │   │      - Brand-token anti-poisoning     │
  │       Bing)            │   │        gate + MX verification         │
  │                        │   │                                     │
  │  1c. Legacy /pub/      │   │  2c. Website crawl for team match:   │
  │      slug recovery     │   │      /, /contact, /about, /team,     │
  │      + name matching   │   │      /people, /about-us               │
  │                        │   │      HTML email extraction +          │
  │  Output:               │   │      first/last name prefix match     │
  │  final_linkedin_url    │   │                                     │
  │  linkedin_resolution_  │   │  2d. 8-pattern deterministic email    │
  │    status              │   │      permutation generation           │
  └────────────────────────┘   │                                     │
                               │  Output:                             │
                               │  final_email                         │
                               │  email_permutations                  │
                               │  verification_status                 │
                               └─────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              OUTPUT LAYER                                          │
│  Appends processed batch to output CSV (mode="a").                 │
│  Header written once on first batch.                               │
│  All original input columns preserved intact.                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Prerequisites

| Dependency | Minimum Version | Notes |
|------------|-----------------|-------|
| Python | 3.10+ | Required for `asyncio` `TaskGroup` patterns and type hints |
| Docker | 20.10+ | Required for local SearXNG container |
| Docker Compose | 1.29+ (v3.7 format) | Used to start the search engine |
| curl-cffi | 0.7.0+ | Provides Chrome-impersonated TLS fingerprint |
| dnspython | 2.6.0+ | Async DNS MX resolver (`dns.asyncresolver`) |
| aiosmtplib | 3.0.0+ | Available but SMTP handshake disabled in current build |
| beautifulsoup4 | 4.12.0+ | HTML parsing for OpenGraph and email extraction |
| playwright | 1.44.0+ | Listed in requirements; not consumed by `pipeline.py` |
| pandas | 2.0.0+ | CSV/TSV ingestion, DataFrame batch writes |

A `.env` file may be present at the repository root for optional API keys (e.g., `APOLLO_API_KEY`). The current pipeline build does not actively consume these keys — they exist as extension points for future data source integrations.

---

## 5. Local Search Service Setup

The SearXNG instance is required for LinkedIn recovery and corporate domain discovery. It must be running before pipeline execution begins.

### 5.1 Start the SearXNG Container

```bash
docker compose up -d searxng
```

Verify health:

```bash
curl -s "http://127.0.0.1:8080/search?q=test&format=json" | head -c 200
```

A valid JSON response with a `results` array confirms the service is operational.

### 5.2 Apply Settings Override

The base image ships with rate limiting enabled. Create a settings override file to disable the limiter and pin the JSON format:

```bash
mkdir -p searxng-settings
```

Create `searxng-settings/settings.yml`:

```yaml
search:
  formats:
    - json
  engines:
    - name: duckduckgo
      disabled: false
    - name: brave
      disabled: false
    - name: yahoo
      disabled: false
    - name: qwant
      disabled: false
    - name: bing
      disabled: false

outgoing:
  useragent_suffix: ""
  max_request_timeout: 4

limiter: false
```

Mount the override into the running container or bake it into `docker-compose.yml`:

```yaml
services:
  searxng:
    container_name: searxng_local
    image: docker.io/searxng/searxng:latest
    restart: unless-stopped
    ports:
      - "127.0.0.1:8080:8080"
    volumes:
      - ./searxng-settings/settings.yml:/etc/searxng/settings.yml:ro
    environment:
      - SEARXNG_BASE_URL=http://127.0.0.1:8080/
    cap_drop:
      - ALL
    cap_add:
      - CHOWN
      - SETGID
      - SETUID
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

Recreate the container after modifying the compose file:

```bash
docker compose up -d --force-recreate searxng
```

### 5.3 Verify Search Engine Availability

```bash
curl -s "http://127.0.0.1:8080/search?q=%22test%20company%22+official+website&format=json&engines=duckduckgo,brave,yahoo,qwant,bing"
```

Expected: HTTP `200` with a JSON body containing non-empty `results`.

---

## 6. CLI Usage & Commands

### 6.1 Basic Execution

```bash
# Single file input
python3 run.py \
  -i "data/raw/input.csv" \
  -o data/processed/output.csv

# Wildcard multi-file input (sorted alphabetically before processing)
python3 run.py \
  -i "data/raw/*.csv" \
  -o data/processed/master_enriched_final.csv

# Explicit batch size override
python3 run.py \
  -i "data/raw/*.csv" \
  -o data/processed/master_enriched_final.csv \
  -b 10
```

The CLI wrapper at `run.py` performs three pre-flight actions before calling into `pipeline.py`:

1. Creates the output directory tree if absent (`os.makedirs(..., exist_ok=True)`).
2. Creates the `logs/` directory.
3. Creates the `.cache/` directory.

### 6.2 CLI Argument Reference

| Flag | Long Flag | Default | Type | Description |
|------|-----------|---------|------|-------------|
| `-i` | `--input` | `data/raw/*.csv` | `str` | Input CSV or TSV path. Supports shell wildcards (`*`, `?`). Multiple files are loaded in sorted filename order and vertically concatenated. |
| `-o` | `--output` | `data/processed/master_enriched_final.csv` | `str` | Destination file path. Parent directories are created automatically. |
| `-b` | `--batch-size` | `15` | `int` | Number of rows processed concurrently per batch. Higher values increase throughput but increase concurrent outbound connections. Recommended range: 5–25 depending on network egress limits. |

### 6.3 Checkpoint / Resume Behavior

Resume is automatic and idempotent. On startup:

1. If the output file already exists, it is read into memory.
2. A composite key is computed for every existing row:
   ```
   key = f"{row_number}|{name}|{email}"
   ```
3. Input rows whose key is absent from the output file are queued for processing.
4. New results are appended to the output file in append mode (`mode="a"`).

To force a full re-run, delete or rename the existing output file before execution:

```bash
mv data/processed/master_enriched_final.csv data/processed/master_enriched_final.csv.bak
python3 run.py -i "data/raw/*.csv" -o data/processed/master_enriched_final.csv
```

### 6.4 Background / Detached Execution

```bash
nohup python3 run.py \
  -i "data/raw/*.csv" \
  -o data/processed/master_enriched_final.csv \
  -b 15 > logs/pipeline_run.log 2>&1 &
```

Monitor:

```bash
tail -f logs/master_enrichment.log
```

---

## 7. Technical Deep-Dive: Pipeline Logic

### 7.1 Profile Verification Gate

The LinkedIn resolution gate applies a three-tier classification based on HTTP response inspection rather than simple status-code checks.

**Tier 1 — Input URL Validation (`LIVE_PROFILE_CONFIRMED`):**

When a valid `linkedin_url` or `linkedin_normalized_url` is present in the input:

1. The URL is canonicalized: protocol normalized to `https`, locale subdomain stripped, query strings and fragments removed, trailing slash dropped, and the slug is validated (minimum 2 characters, not purely numeric).
2. A GET request is issued using a Slackbot link-expansion User-Agent (`PROBE_HEADERS`). This bypasses the standard LinkedIn authwall and returns the OpenGraph metadata without requiring login.
3. The response is classified:
   - **Hard tombstone (excluded):** HTTP `404`, `410`, or final URL path containing `/404`. OpenGraph `<meta property="og:title">` or `<title>` containing `"page not found"`, `"profile not found"`, or `"profile unavailable"`.
   - **Live (accepted):** HTTP `200` with valid OG title that does not match tombstone strings.
   - **Soft authwall / interstitial (accepted with caveat):** HTTP `200` where the page redirects to a login interstitials. The pipeline returns `True` on any non-404, non-tombstone response to avoid false negatives. This is a deliberate trade-off: over-enrichment is preferred over false exclusion.

Exceptions during probe (timeouts, connection resets) default to `True` (assume live) to prevent valid profiles from being discarded on transient network failure.

**Tier 2 — SearXNG Search Recovery (`SEARXNG_VERIFIED_LIVE`):**

When no valid input URL exists or the probe fails, the pipeline executes a waterfall of SearXNG queries:

1. **Company-targeted queries** using up to three brand-variant truncations of the company name (e.g., `"Acme Technologies Inc"` → `"acme technologies"`, `"acme"`). Queries are quoted-phrase exact-match to reduce noise.
2. **Legacy `/pub/` hint query:** If the input row contains a deprecated `/pub/` LinkedIn URL, the slug is decoded and used as an additional bare-name search query.
3. **Broader fallback queries:** Unquoted name + company variant, and `site:linkedin.com/in/` exact-path filter.

Each SearXNG result is checked against `LINKEDIN_IN_REGEX`. The first candidate whose slug passes `is_strict_name_match` is returned. The strict matcher tolerates middle names, name prefixes (`de`, `da`, `van`, `lo`), and consonant-only transliteration comparison.

**Tier 3 — Unresolved:**

No URL recovered after all query tiers exhaust. Field is set to empty string. Status: `LINKEDIN_UNRESOLVED`.

### 7.2 Corporate Domain Discovery & Anti-Poisoning

Domain discovery follows a strict priority chain, with an anti-poisoning gate at every step:

**Priority Chain:**

1. **Enterprise rebrand aliases (`KNOWN_ENTERPRISE_DOMAINS`):** Hard-coded mapping of ~10 known company rebrandings (e.g., `"idea cellular"` → `"myvi.in"`). Used as a fast-path before any network call. If the cleaned company name matches an alias key and the mapped domain has valid MX records, the alias domain is returned immediately.

2. **Clearbit autocomplete API:** Queries `https://autocomplete.clearbit.com/v1/companies/suggest?query=<company>`. Returns the top suggestion domain. Subject to brand-match gate and MX verification.

3. **SearXNG "official website" search:** Queries SearXNG with `"<company>" official website` across DuckDuckGo, Brave, Yahoo, and Qwant engines. Extracts domain from result URLs. Subject to brand-match gate and MX verification.

**Anti-Poisoning Gate (`is_domain_brand_match`):**

Every discovered domain passes through this gate before acceptance. It prevents three classes of false positives:

- **Scraper directory poisoning:** Domains from sites like zoominfo.com, apollo.io, rocketreach.co, yellowpages.com are in `DIRECTORY_BLACKLIST` and are rejected outright regardless of brand match.
- **Parked-domain poisoning:** A domain like `somedomain-parked.com` might match a company token superficially but have no real MX. The MX verification step filters these.
- **Homograph / partial-token poisoning:** The gate requires at least one company token (3+ characters) to be a substring of the domain root, or the domain root to be a substring of a company token. An acronym check (`first letters of all company words`, minimum length 3) provides additional coverage for initialism-heavy company names (e.g., `"International Business Machines"` → acronym `ibm`).

### 7.3 DNS MX Verification & Email Permutations

**MX Resolution (`get_mx_hosts`):**

- Uses `dns.asyncresolver.Resolver` with hardcoded public resolvers: `1.1.1.1` (Cloudflare), `8.8.8.8` (Google), `9.9.9.9` (Quad9).
- Timeout and lifetime both set to 1.2 seconds to prevent firewall socket hangs on unreachable resolvers.
- Results are cached in-memory (`self.mx_cache`) for the lifetime of the process. No disk persistence.
- Records are sorted by MX preference before return.

**Subdomain fallback:** The current implementation does not probe subdomains (e.g., `mail.company.com`) as a fallback. Only the apex domain is checked. This is a deliberate design choice to reduce DNS query volume per record.

**Email Permutation Engine (`generate_email_permutations`):**

Generates exactly 8 permutations when both first and last name are available, ordered by observed corporate email format frequency:

| Priority | Format | Example | Approximate Frequency |
|----------|--------|---------|----------------------|
| 1 | `first.last@domain` | `john.smith@company.com` | 50% |
| 2 | `flast@domain` | `jsmith@company.com` | 30% |
| 3 | `first_last@domain` | `john_smith@company.com` | 8% |
| 4 | `firstlast@domain` | `johnsmith@company.com` | 5% |
| 5 | `first.l@domain` | `john.s@company.com` | 3% |
| 6 | `f.last@domain` | `j.smith@company.com` | 2% |
| 7 | `last.first@domain` | `smith.john@company.com` | 1% |
| 8 | `first@domain` | `john@company.com` | 1% |

If only a first name is available (no last name), a single permutation `first@domain` is generated.

### 7.4 Website Team Crawl

After domain discovery and MX verification, the pipeline probes up to 6 endpoints on the corporate domain:

```
/, /contact, /about, /team, /people, /about-us
```

For each endpoint returning HTTP `200`, the HTML is parsed and all email-like strings matching the domain are extracted. Generic prefixes (`info`, `contact`, `support`, `sales`, `admin`, `hr`, etc.) are excluded. The remaining emails are checked for a first-name substring match (and last-name substring match if available). The first match is returned as `SCRAPED_TEAM_MATCH`. If no match is found, the top-ranked permutation is returned as `MX_VERIFIED_PATTERN`.

### 7.5 Self-Employed / Freelance Classification

The `GENERIC_COMPANIES` set contains 20+ normalized strings across English, Spanish, Portuguese, Italian, and French indicating self-employment or non-corporate affiliation. When a company field matches any entry (after ASCII normalization and case folding), the email resolution stage short-circuits: `final_email` and `email_permutations` are set to empty, and `verification_status` is set to `SELF_EMPLOYED_NO_DOMAIN`. LinkedIn resolution proceeds independently.

---

## 8. Input Schema

The pipeline accepts CSV and TSV files with the following recognized columns. Column names are normalized to lowercase and whitespace-stripped on ingestion. Extra columns are preserved in the output.

| Column Name | Required | Description |
|-------------|----------|-------------|
| `row_number` | No | Row index identifier. Used in checkpoint key computation. |
| `name` | Yes | Full name of the subject. Used for LinkedIn search queries and email permutation generation. |
| `email` | Yes | Existing or placeholder email address. Domain portion is used for MX verification. |
| `company` | Yes | Company or organization name. Used for domain discovery and SearXNG queries. |
| `linkedin_url` | Yes | Raw or legacy LinkedIn URL. May be `/pub/` format, `/in/` format, or non-ASCII encoded. |
| `linkedin_normalized_url` | No | Secondary clean LinkedIn URL field. Used if `linkedin_url` is absent. |
| `linkedin_verified` | No | Pre-existing verification flag. Passed through to output. |
| `email_status` | No | Pre-existing email status. Passed through. |
| `email_score` | No | Pre-existing quality score. Passed through. |
| `email_verified` | No | Pre-existing verification flag. Passed through. |
| `email_reasons` | No | Diagnostic codes from prior tools. Passed through. |
| `linkedin_status` | No | Pre-existing LinkedIn status. Passed through. |
| `linkedin_reason` | No | Disqualification code. Passed through. |

Delimiter detection is automatic: the first line of each file is inspected; if a tab character is found, TSV mode is activated; otherwise CSV mode is used.

---

## 9. Output Schema & Status Reference

All original input columns are preserved. Four fields are appended:

| Field | Type | Description |
|-------|------|-------------|
| `final_linkedin_url` | `str` | Canonical `https://www.linkedin.com/in/<slug>` URL or empty string. |
| `linkedin_resolution_status` | `str` | One of three resolution outcome codes (see table below). |
| `final_email` | `str` | Recovered email address or empty string. |
| `email_permutations` | `str` | Semicolon-delimited ordered list of all generated permutations. |
| `verification_status` | `str` | One of four verification outcome codes (see table below). |
| `source_file` | `str` | Filename of the originating input CSV/TSV. Added automatically. |

### 9.1 `linkedin_resolution_status` Codes

| Code | Meaning |
|------|---------|
| `LIVE_PROFILE_CONFIRMED` | Input URL was canonicalized, live-probed via OpenGraph HTTP GET, and returned a non-tombstone HTTP 200 response. |
| `SEARXNG_VERIFIED_LIVE` | Input URL absent or tombstoned. Profile recovered via SearXNG multi-engine waterfall. Name-matched against slug before acceptance. |
| `LINKEDIN_UNRESOLVED` | No valid profile found after all resolution tiers. Field is empty. |

### 9.2 `verification_status` Codes

| Code | Meaning |
|------|---------|
| `MX_VERIFIED_PATTERN` | Corporate domain resolved (via enterprise alias, Clearbit, or SearXNG). Domain has active MX records. No scraped email match found on corporate website. `final_email` contains the highest-probability permutation. |
| `SCRAPED_TEAM_MATCH` | Corporate website crawl found an email containing the subject's first and/or last name. `final_email` is the scraped address. Highest-confidence outcome. |
| `SELF_EMPLOYED_NO_DOMAIN` | Company field matched `GENERIC_COMPANIES` set. No domain lookup attempted. `final_email` is empty. |
| `UNRESOLVED` | No MX-verified domain found after all discovery tiers. `final_email` is empty. |

### 9.3 `email_permutations` Format

Semicolon-delimited (`; `) ordered list of all 8 generated permutations. The highest-confidence format (`first.last@domain`) is listed first. If a `SCRAPED_TEAM_MATCH` is found, the scraped email is prepended to the permutation list:

```
john.smith@acme.com; john.smith@acme.com; jsmith@acme.com; john_smith@acme.com; ...
```

---

## 10. Post-Processing & Data Extraction

### 10.1 Status Code Aggregation

```bash
# Count records by LinkedIn resolution status
python3 -c "
import pandas as pd
df = pd.read_csv('data/processed/master_enriched_final.csv')
print(df['linkedin_resolution_status'].value_counts().to_string())
"
```

```bash
# Count records by email verification status
python3 -c "
import pandas as pd
df = pd.read_csv('data/processed/master_enriched_final.csv')
print(df['verification_status'].value_counts().to_string())
"
```

### 10.2 Filter to High-Confidence Deliverables

```bash
# Export only rows with verified MX and confirmed LinkedIn
python3 -c "
import pandas as pd
df = pd.read_csv('data/processed/master_enriched_final.csv')
high_conf = df[
    (df['verification_status'].isin(['MX_VERIFIED_PATTERN', 'SCRAPED_TEAM_MATCH'])) &
    (df['linkedin_resolution_status'].isin(['LIVE_PROFILE_CONFIRMED', 'SEARXNG_VERIFIED_LIVE']))
]
high_conf.to_csv('data/processed/high_confidence_enriched.csv', index=False)
print(f'Exported {len(high_conf)} / {len(df)} high-confidence records')
"
```

```bash
# Export only scraped team matches (highest email confidence)
python3 -c "
import pandas as pd
df = pd.read_csv('data/processed/master_enriched_final.csv')
scraped = df[df['verification_status'] == 'SCRAPED_TEAM_MATCH']
scraped.to_csv('data/processed/scraped_team_matches.csv', index=False)
print(f'Exported {len(scraped)} scraped team matches')
"
```

### 10.3 Self-Employed Isolation

```bash
python3 -c "
import pandas as pd
df = pd.read_csv('data/processed/master_enriched_final.csv')
self_emp = df[df['verification_status'] == 'SELF_EMPLOYED_NO_DOMAIN']
print(f'Self-employed / freelance records: {len(self_emp)}')
self_emp.to_csv('data/processed/self_employed_cohort.csv', index=False)
"
```

---

## 11. Troubleshooting & Operations Guide

### 11.1 SearXNG Returns 403 Forbidden

**Symptom:** SearXNG queries return HTTP 403. SearXNG container logs show upstream engine blocks.

**Resolution:**

1. Check if SearXNG's built-in rate limiter is active:
   ```bash
   curl -s "http://127.0.0.1:8080/search?q=test&format=json" -I | grep -i "x-ratelimit"
   ```
2. Verify `settings.yml` has `limiter: false` set at the top level of the YAML document.
3. Recreate the container after modifying the settings mount:
   ```bash
   docker compose up -d --force-recreate searxng
   ```
4. Check SearXNG instance logs:
   ```bash
   docker logs searxng_local --tail 50
   ```

### 11.2 DNS Resolver Timeouts / MX Lookups Return Empty

**Symptom:** All domains return `UNRESOLVED` or `MX_VERIFIED_PATTERN` with empty permutations despite known-valid domains.

**Resolution:**

1. Verify outbound UDP port 53 is not blocked:
   ```bash
   dig @1.1.1.1 MX google.com +timeout=2 +tries=1
   dig @8.8.8.8 MX google.com +timeout=2 +tries=1
   ```
2. If corporate firewalls block external DNS, override `resolver.nameservers` in `pipeline.py` at the `get_mx_hosts` method (line 343) with internal resolvers.
3. Confirm `dns.asyncresolver` is not hitting a local caching stub that is stale. Restart the pipeline to clear `self.mx_cache` (in-memory only, no persistence issue).

### 11.3 SearXNG Returns No Results

**Symptom:** SearXNG responds HTTP 200 but `results` array is empty for all queries.

**Resolution:**

1. Test the SearXNG endpoint directly:
   ```bash
   curl -s "http://127.0.0.1:8080/search?q=%22acme+inc%22+official+website&format=json&engines=duckduckgo,brave,yahoo,qwant"
   ```
2. Check engine availability inside the container:
   ```bash
   docker exec searxng_local searxng -d 2>&1 | grep -i "disabled\|engine"
   ```
3. Rebuild the SearXNG image if settings are not being applied:
   ```bash
   docker compose build searxng && docker compose up -d searxng
   ```

### 11.4 Pipeline Stalls or Hangs

**Symptom:** Process appears frozen; no log output for >60 seconds.

**Resolution:**

1. The `aiohttp` session timeout for HTTP probes is 2.5s and for SearXNG is 4.0s. Stalls are typically caused by DNS resolution timeouts (1.2s lifetime). The in-memory semaphores (`probe_sem=8`, `searxng_sem=2`) prevent unbounded concurrency, but a batch of 15 with all MX lookups timing out will take ~18 seconds per batch.
2. Reduce batch size to isolate the problem:
   ```bash
   python3 run.py -i "data/raw/*.csv" -o data/processed/out.csv -b 5
   ```
3. Check for blocking calls in custom extensions. All I/O in `pipeline.py` is async. Any added synchronous code (e.g., `requests.get`, `time.sleep` >1s) will block the event loop.

### 11.5 Delimiter Detection Fails

**Symptom:** Columns are misaligned; all data appears in a single column.

**Resolution:**

The delimiter is detected by inspecting the first line of each file. If the first line contains a tab, TSV mode is activated. If a file has a non-standard delimiter (e.g., pipe `|`), detection will default to CSV. Pre-process the file to standardize the delimiter, or modify `sep = "\t" if "\t" in first_line else ","` in `load_all_csvs` (line 554) to support additional delimiters.

### 11.6 Resume Produces Duplicate Rows

**Symptom:** After resuming, the output file contains duplicate rows for records that were partially written in a previous interrupted run.

**Resolution:**

The checkpoint key is computed from `row_number|name|email`. If any of these fields contain leading/trailing whitespace or non-ASCII characters that normalize differently between runs, the key will not match and the row will be reprocessed. Strip whitespace in the source data, or normalize these fields before feeding them to the pipeline. The `clean_text_to_ascii` helper normalizes Unicode but does not strip whitespace from the raw input — this is intentional to preserve exact row identification.

---

## 12. Constants Reference

### 12.1 `DIRECTORY_BLACKLIST`

Domains excluded from acceptance regardless of brand-match score. Prevents data pollution from data-aggregation platforms, parked domains, and social media sites.

```
linkedin.com, facebook.com, instagram.com, twitter.com, x.com,
wikipedia.org, youtube.com, bloomberg.com, crunchbase.com,
glassdoor.com, indeed.com, zoominfo.com, pitchbook.com,
contactout.com, leadiq.com, visualvisitor.com, apollo.io, dnb.com,
rocketreach.co, lusha.com, yellowpages.com, yelp.com, signalhire.com,
adapt.io, upcountry.com, kompass.com, dottorpaolo.com, pearsonfamily.id.au
```

### 12.2 `GENERIC_COMPANIES`

Strings indicating self-employment or non-corporate affiliation. Matched against the ASCII-normalized, lowercased company field.

```
selfemployed, independiente, on my own, self employed,
freelance, freelancer, independent, none, n/a, na,
null, nocompany, no company, nan, consultant, self,
retired, unemployed, em casa, autnoma, autonoma,
autonomo, cuenta propia, particular, my own, free lance
```

### 12.3 `GENERIC_EMAIL_PREFIXES`

Email local-parts excluded from scraped team-match results. Prevents generic mailbox addresses from being misattributed to an individual.

```
info, contact, support, sales, admin, hello, help,
careers, jobs, office, general, team, privacy, billing,
press, media, service, customerservice, enquiries, hr
```

### 12.4 `KNOWN_ENTERPRISE_DOMAINS`

Hard-coded rebrand alias map for enterprise domains where the legal entity name differs from the operational email domain.

| Legal Entity Name | Operational Domain |
|-------------------|-------------------|
| `idea cellular` | `myvi.in` |
| `vodafone idea` | `myvi.in` |
| `tata docomo` | `tatatelebusiness.com` |
| `bbva compass` | `bbvausa.com` |
| `nextel brasil` | `claro.com.br` |
| `uk department of health` | `dhsc.gov.uk` |
| `east london nhs` | `elft.nhs.uk` |
| `gateshead health nhs` | `gatesheadhealth.nhs.uk` |
| `community health systems` | `chs.net` |
| `danafarber` | `dfci.harvard.edu` |
| `clark county school district` | `ccsd.net` |

### 12.5 DNS Resolvers

The pipeline uses three public DNS resolvers in round-robin failover:

```
1.1.1.1   (Cloudflare)
8.8.8.8   (Google Public DNS)
9.9.9.9   (Quad9)
```

Resolver timeout: **1.2 seconds**. Resolver lifetime: **1.2 seconds**.

---

## 13. Concurrency & Performance Characteristics

| Parameter | Default | Effective Range | Notes |
|-----------|---------|-----------------|-------|
| `--batch-size` (`-b`) | 15 | 1–50 | Total concurrent tasks per batch. Each row spawns up to 6–8 HTTP requests (MX, Clearbit, SearXNG, 6 website endpoints, LinkedIn probe). |
| `probe_sem` | 8 | Hardcoded | Maximum concurrent HTTP probes (LinkedIn + website crawl). |
| `searxng_sem` | 2 | Hardcoded | Maximum concurrent SearXNG queries. Prevents upstream rate-limiting. |
| DNS timeout | 1.2s | Hardcoded | Per-query timeout. Non-negotiable without code change. |
| MX cache | In-memory | Process lifetime | No disk persistence. Cache is cold on every new process invocation. |

**Expected throughput:** At `-b 15` with all MX lookups hitting cached domains, ~15 rows complete in 3–8 seconds. First-run throughput is lower due to cold DNS cache and SearXNG latency.

---

## 14. Logging

All execution events are written to `logs/master_enrichment.log` (append mode, UTF-8) and streamed to `stdout`.

Log format:

```
2026-08-17 13:03:12,345 [INFO] [1/1500] John Smith (Acme Corp) -> Email: john.smith@acme.com [MX_VERIFIED_PATTERN] | LinkedIn: https://www.linkedin.com/in/john-smith [LIVE_PROFILE_CONFIRMED]
2026-08-17 13:03:12,567 [INFO] [SEARXNG HIT] Jane Doe (Beta Ltd) -> https://www.linkedin.com/in/jane-doe-beta
2026-08-17 13:03:13,890 [INFO] [VALID DOMAIN FOUND] Gamma Inc -> gamma.example.com
2026-08-17 13:03:14,123 [INFO] [SCRAPED TEAM HIT] Bob Lee (Delta LLC) -> bob.lee@delta.example.com
```

Log level is hardcoded to `INFO` in `pipeline.py` (line 31). Change `logging.basicConfig(level=...)` to `DEBUG` for per-request HTTP and DNS detail.
