import asyncio
import logging
import os
import re
import unicodedata
import urllib.parse
from typing import Dict, Any, Optional, Tuple

import aiohttp
import pandas as pd

# Configure Production Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("linkedin_verification.log"),
        logging.StreamHandler(),
    ],
)

LEGAL_SUFFIXES = re.compile(
    r"\b(pvt ltd|private limited|ltd|inc|llc|group|foundation trust|dept|department|pt|gmbh|corp|corporation|llp|sa|sl|srl|pvt)\b",
    re.IGNORECASE,
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def clean_text_to_ascii(text: str) -> str:
    """Decodes percent-encoding and normalizes Unicode accents (é -> e, á -> a)."""
    if not text or pd.isna(text):
        return ""
    decoded = urllib.parse.unquote(str(text))
    nfkd = unicodedata.normalize("NFKD", decoded)
    return nfkd.encode("ASCII", "ignore").decode("utf-8").lower().strip()


def sanitize_company_name(company: str) -> str:
    """Strips legal suffixes and formatting noise from company names."""
    if not company or pd.isna(company):
        return ""
    clean = clean_text_to_ascii(str(company))
    clean = LEGAL_SUFFIXES.sub("", clean)
    return re.sub(r"\s+", " ", clean).strip()


def is_self_employed(company: str) -> bool:
    """Identifies self-employed or freelance leads."""
    if not company or pd.isna(company):
        return True
    comp_lower = str(company).lower().strip()
    return comp_lower in {
        "selfemployed", "independiente", "on my own", 
        "self employed", "freelance", "freelancer", "independent"
    }


async def verify_url_live(session: aiohttp.ClientSession, url: str) -> bool:
    """
    Sends an HTTP GET request to verify if the LinkedIn URL is live (200 OK)
    and not returning a 404 Page Not Found error.
    """
    if not url or "linkedin.com" not in url:
        return False
    try:
        async with session.get(
            url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=4), allow_redirects=True
        ) as resp:
            # LinkedIn returns 200 for valid pages and 404 for missing profiles
            if resp.status == 200 and "404" not in str(resp.url):
                return True
            return False
    except Exception:
        return False


def build_candidate_urls(raw_url: str, name: str, company: str) -> list[Tuple[str, str]]:
    """Generates candidate URLs ordered from most reliable to company fallback."""
    candidates = []
    clean_name = clean_text_to_ascii(name)
    clean_comp = sanitize_company_name(company)
    decoded_url = clean_text_to_ascii(raw_url)

    # Candidate 1: Decoded from Legacy /pub/
    if "/pub/" in decoded_url:
        match = re.search(r"linkedin\.com/pub/([^/]+)", decoded_url)
        if match:
            clean_slug = re.sub(r"[^a-z0-9-]", "", match.group(1)).strip("-")
            if clean_slug and len(clean_slug) > 2:
                candidates.append(
                    (f"https://www.linkedin.com/in/{clean_slug}", "PROFILE_RESOLVED_FROM_LEGACY_PUB")
                )

    # Candidate 2: Cleaned from /in/ URL
    if "/in/" in decoded_url:
        match = re.search(r"linkedin\.com/in/([^/?#]+)", decoded_url)
        if match:
            clean_slug = re.sub(r"[~]+", "-", match.group(1))
            clean_slug = re.sub(r"[^a-z0-9-]", "-", clean_slug)
            clean_slug = re.sub(r"-+", "-", clean_slug).strip("-")
            if clean_slug and not clean_slug.isdigit() and len(clean_slug) > 2:
                candidates.append(
                    (f"https://www.linkedin.com/in/{clean_slug}", "PROFILE_RESOLVED_CANONICAL")
                )

    # Candidate 3: Company Page Fallback (Mentor Directive)
    if clean_comp and not is_self_employed(company):
        comp_slug = "-".join(re.sub(r"[^a-z0-9\s]", "", clean_comp).split())
        if comp_slug:
            candidates.append(
                (f"https://www.linkedin.com/company/{comp_slug}", "COMPANY_PAGE_RESOLVED")
            )

    return candidates


async def process_row(
    row: Dict[str, Any], session: aiohttp.ClientSession
) -> Dict[str, Any]:
    record = dict(row)
    raw_url = str(record.get("linkedin_url", "")).strip()
    name = str(record.get("name", "")).strip()
    company = str(record.get("company", "")).strip()

    candidates = build_candidate_urls(raw_url, name, company)

    # Test each candidate URL against the HTTP verification gate
    for candidate_url, status in candidates:
        is_live = await verify_url_live(session, candidate_url)
        if is_live:
            record["final_linkedin_url"] = candidate_url
            record["linkedin_resolution_status"] = status
            return record

    # Zero-Guess Fallback: Leave blank if no candidate URL is verified live
    record["final_linkedin_url"] = None
    record["linkedin_resolution_status"] = "LINKEDIN_UNRESOLVED"
    return record


async def run_pipeline(input_file: str, output_file: str, batch_size: int = 5):
    logging.info(f"Starting Verified LinkedIn Resolution Pipeline on: {input_file}")

    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    if os.path.exists(output_file):
        os.remove(output_file)

    try:
        df = pd.read_csv(input_file, sep="\t")
    except Exception:
        df = pd.read_csv(input_file)

    records = df.to_dict(orient="records")
    total_batches = (len(records) + batch_size - 1) // batch_size

    connector = aiohttp.TCPConnector(limit=batch_size, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(records), batch_size):
            batch_num = (i // batch_size) + 1
            batch = records[i : i + batch_size]
            logging.info(f"Processing batch {batch_num} / {total_batches}...")

            tasks = [process_row(row, session) for row in batch]
            batch_results = await asyncio.gather(*tasks)

            batch_df = pd.DataFrame(batch_results)
            is_first_batch = (i == 0)
            batch_df.to_csv(
                output_file, mode="a", sep=",", index=False, header=is_first_batch
            )
            logging.info(f"Batch {batch_num} saved to {output_file}")

    logging.info(f"Pipeline complete! Output written to: {output_file}")


if __name__ == "__main__":
    INPUT_PATH = "raw_data/email_verified_only.csv"
    OUTPUT_PATH = "processed/email_verified_only.csv"

    asyncio.run(run_pipeline(INPUT_PATH, OUTPUT_PATH, batch_size=5))