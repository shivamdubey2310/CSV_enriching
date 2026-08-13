# Technical Guide

A high-concurrency asynchronous Python pipeline for **Email Verification** (DNS MX + Port 25 SMTP) and **LinkedIn Profile/Company URL Resolution** (Canonicalization + Live HTTP Status Pings).

## Project Directory Structure

```text
├── pipeline_execution.log
├── pipeline.py
├── processed
│   ├── email_verified_only.csv
│   └── linkedin_verified_only.csv
└── raw_data
    ├── email_verified_only.csv
    └── linkedin_verified_only.csv

3 directories, 6 files
```

## File Dictionary

| Path                                   | Description                                                                            |
| -------------------------------------- | -------------------------------------------------------------------------------------- |
| `pipeline.py`                          | Core unified execution script containing both email verification and LinkedIn engines. |
| `pipeline_execution.log`               | Production runtime log capturing batch timing, DNS lookups, HTTP pings, and errors.    |
| `raw_data/linkedin_verified_only.csv`  | **Dataset 1 Input:** Records with valid LinkedIn profiles needing email recovery.      |
| `raw_data/email_verified_only.csv`     | **Dataset 2 Input:** Records with 100% valid emails needing LinkedIn URL repairs.      |
| `processed/linkedin_verified_only.csv` | **Dataset 1 Output:** Enriched with verified emails and diagnostic status codes.       |
| `processed/email_verified_only.csv`    | **Dataset 2 Output:** Enriched with live, working LinkedIn URLs.                       |

## Expected Input Data Schema

The pipeline automatically supports both **CSV (comma-separated)** and **TSV (tab-separated)** files.

The input dataset expects the following headers:

### Required & Optional Input Columns

| Column Name               | Required | Description / Usage in Pipeline                                                      |
| ------------------------- | -------- | ------------------------------------------------------------------------------------ |
| `row_number`              | No       | Identifier row index. Preserved as-is.                                               |
| `name`                    | Yes      | Full name of the lead (used for SMTP pattern generation and profile search).         |
| `email`                   | Yes      | Initial email address to verify via DNS MX and SMTP Port 25 handshake.               |
| `company`                 | Yes      | Company or organization name (used for website discovery and company page fallback). |
| `linkedin_url`            | Yes      | Raw or legacy LinkedIn URL (e.g., `/pub/`, `/sales/`, or numeric slug).              |
| `linkedin_normalized_url` | No       | Secondary clean or pre-normalized URL field.                                         |
| `linkedin_verified`       | No       | Flag (`yes`/`no`) indicating if the current LinkedIn link is already confirmed live. |
| `email_status`            | No       | Pre-existing status flag (e.g., `valid`, `risky`). Preserved as-is.                  |
| `email_score`             | No       | Quality score. Preserved as-is.                                                      |
| `email_verified`          | No       | Verification flag (`yes`/`no`).                                                      |
| `email_reasons`           | No       | Diagnostic reason code from previous tools.                                          |
| `linkedin_status`         | No       | Status flag.                                                                         |
| `linkedin_reason`         | No       | Disqualification code (e.g., `missing_in_slug`, `bad_slug_characters`).              |
| `linkedin_normalized_url` | No       | Clean or pre-normalized LinkedIn URL, when available.                                |

### Sample Raw Input Format

```csv
row_number,name,email,linkedin_url,company,email_status,email_score,email_verified,email_reasons,linkedin_status,linkedin_verified,linkedin_reason,linkedin_normalized_url
87,Andre Lins,andre.lins@edax.com.br,http://www.linkedin.com/pub/andr%c3%a9-lins/24/a73/1b,edax tecnologia,valid,100,yes,,invalid,no,missing_in_slug,https://www.linkedin.com/pub/andr%c3%a9-lins/24/a73/1b
2471,David Murray,dmurray@ardorhealth.com,http://www.linkedin.com/pub/david-murray/12a/947/981,ardor health solutions,valid,100,yes,,invalid,no,missing_in_slug,https://www.linkedin.com/pub/david-murray/12a/947/981
```

## Quick Start & Prerequisites

### 1. Requirements

* Python 3.10+
* Virtual Environment

### 2. Dependency Installation

```bash
# Activate virtual environment
source .venv/bin/activate

# Install required dependencies
pip install aiohttp aiosmtplib dnspython beautifulsoup4 pandas
```

## How to Run the Pipeline

The script `pipeline.py` automatically handles both datasets and accepts custom CLI flags.

### Command 1: Recover Missing Emails

Input:

`raw_data/linkedin_verified_only.csv`

Output:

`processed/linkedin_verified_only.csv`

```bash
python3 pipeline.py \
  -i raw_data/linkedin_verified_only.csv \
  -o processed/linkedin_verified_only.csv \
  -b 5
```

This workflow verifies email addresses using:

1. DNS MX record resolution.
2. SMTP Port 25 handshakes.
3. Search engine extraction for candidate email addresses.

### Command 2: Fix Broken LinkedIn URLs

Input:

`raw_data/email_verified_only.csv`

Output:

`processed/email_verified_only.csv`

```bash
python3 pipeline.py \
  -i raw_data/email_verified_only.csv \
  -o processed/email_verified_only.csv \
  -b 5
```

This workflow:

1. Cleans legacy LinkedIn links such as `/pub/` and `/sales/`.
2. Handles non-ASCII characters.
3. Resolves canonical LinkedIn profile URLs.
4. Confirms live HTTP `200 OK` responses.
5. Falls back to verified company pages when an individual profile cannot be resolved.

### Command 3: Background Production Mode

For large datasets, run the process in the background detached from the terminal:

```bash
nohup python3 pipeline.py \
  -i raw_data/linkedin_verified_only.csv \
  -o processed/linkedin_verified_only.csv \
  -b 10 > pipeline_execution.log 2>&1 &
```

## Command-Line Flags

| Flag | Long Flag      | Default Value                         | Description                              |
| ---- | -------------- | ------------------------------------- | ---------------------------------------- |
| `-i` | `--input`      | `raw_data/linkedin_verified_only.csv` | Input CSV/TSV source file path.          |
| `-o` | `--output`     | `processed/enriched_output.csv`       | Output destination file path.            |
| `-b` | `--batch-size` | `5`                                   | Number of concurrent requests per batch. |

## Enriched Output Data Schema

The pipeline retains **all original input columns** and appends four diagnostic fields:

```text
[Original Headers...]
+ final_email
+ verification_status
+ final_linkedin_url
+ linkedin_resolution_status
```

## Diagnostic Status Codes

### Email Verification

| Status Code               | Description                                                         |
| ------------------------- | ------------------------------------------------------------------- |
| `SMTP_HANDSHAKE_VERIFIED` | Confirmed live mailbox via direct Port 25 SMTP `250 OK` handshake.  |
| `MX_VERIFIED`             | Active DNS MX mail servers resolved for the domain.                 |
| `SEARCH_SMTP_VERIFIED`    | Search candidate verified live via SMTP.                            |
| `SELF_EMPLOYED_NO_DOMAIN` | Lead identified as freelancer/self-employed with no company domain. |
| `UNRESOLVED`              | Domain dead or unverified; left blank to prevent bounces.           |

### LinkedIn Resolution

| Status Code                        | Description                                                                 |
| ---------------------------------- | --------------------------------------------------------------------------- |
| `LINKEDIN_ORIGINAL_VALID`          | Original input URL confirmed live (`200 OK`).                               |
| `PROFILE_RESOLVED_FROM_LEGACY_PUB` | Successfully converted `/pub/` directory path to an active personal handle. |
| `PROFILE_RESOLVED_CANONICAL`       | Cleaned and validated canonical `/in/` profile link.                        |
| `COMPANY_PAGE_RESOLVED`            | Personal profile missing; resolved to a verified official Company Page.     |
| `LINKEDIN_UNRESOLVED`              | Profile unavailable; left blank to prevent `404` broken links.              |
