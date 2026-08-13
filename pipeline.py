import argparse
import asyncio
import logging
import os
import re
import sys
import unicodedata
import urllib.parse
from typing import Dict, Any, Optional, Set, List, Tuple

import aiohttp
import aiosmtplib
import dns.asyncresolver
from bs4 import BeautifulSoup
import pandas as pd

# Configure Production Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline_execution.log"),
        logging.StreamHandler(),
    ],
)

# Regex Patterns
STANDARD_EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", re.IGNORECASE
)
LINKEDIN_PROFILE_REGEX = re.compile(
    r"linkedin\.com/in/([a-zA-Z0-9%_-]+)", re.IGNORECASE
)
LINKEDIN_COMPANY_REGEX = re.compile(
    r"linkedin\.com/company/([a-zA-Z0-9%_-]+)", re.IGNORECASE
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

INVALID_TLDS = {
    "href", "push", "our", "png", "jpg", "jpeg", "svg", "gif", 
    "webp", "css", "js", "json", "html", "php", "aspx", "ts"
}


def clean_text_to_ascii(text: Any) -> str:
    """Decodes percent-encoding and normalizes Unicode accents (é -> e, á -> a)."""
    if pd.isna(text) or not text:
        return ""
    decoded = urllib.parse.unquote(str(text))
    nfkd = unicodedata.normalize("NFKD", decoded)
    return nfkd.encode("ASCII", "ignore").decode("utf-8").lower().strip()


def sanitize_company_name(company: Any) -> str:
    """Strips legal suffixes and formatting noise from company names."""
    clean = clean_text_to_ascii(company)
    if not clean:
        return ""
    clean = LEGAL_SUFFIXES.sub("", clean)
    return re.sub(r"\s+", " ", clean).strip()


def is_self_employed(company: Any) -> bool:
    """Identifies self-employed or freelance leads."""
    comp_lower = clean_text_to_ascii(company)
    if not comp_lower:
        return True
    return comp_lower in {
        "selfemployed", "independiente", "on my own", 
        "self employed", "freelance", "freelancer", "independent"
    }


class UnifiedDataPipeline:
    def __init__(self, timeout_seconds: int = 5):
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._mx_cache: Dict[str, List[str]] = {}

    def _get_dns_resolver(self) -> dns.asyncresolver.Resolver:
        resolver = dns.asyncresolver.Resolver()
        resolver.nameservers = ["8.8.8.8", "1.1.1.1"]
        resolver.lifetime = 3.0
        return resolver

    # --- EMAIL ENRICHMENT ENGINE ---

    async def get_mx_records(self, domain: str) -> List[str]:
        """Resolves active MX records for a domain using Google/Cloudflare DNS."""
        if not domain or pd.isna(domain) or "." not in str(domain):
            return []

        clean_domain = str(domain).strip().lower()
        if clean_domain in self._mx_cache:
            return self._mx_cache[clean_domain]

        try:
            resolver = self._get_dns_resolver()
            answers = await resolver.resolve(clean_domain, "MX")
            records = [str(r.exchange).rstrip(".") for r in answers]
            if records:
                self._mx_cache[clean_domain] = records
                return records
        except Exception:
            pass

        self._mx_cache[clean_domain] = []
        return []

    async def verify_smtp_mailbox(self, email: str, mx_host: str) -> Optional[bool]:
        """Performs asynchronous SMTP handshake (RCPT TO) for strict verification."""
        sender = "verify@check-domain.com"
        try:
            smtp = aiosmtplib.SMTP(hostname=mx_host, port=25, timeout=3.5, tls_context=None)
            await smtp.connect()
            await smtp.helo()
            await smtp.mail(sender)

            code, _ = await smtp.rcpt(email)
            await smtp.quit()

            if code == 250:
                return True
            elif code in (550, 551, 552, 553, 554):
                return False
            return None
        except Exception:
            return None

    def extract_clean_emails(self, text: str) -> Set[str]:
        """Strips JS noise and extracts valid emails."""
        if not text:
            return set()

        soup = BeautifulSoup(text, "html.parser")
        for script in soup(["script", "style", "noscript", "svg"]):
            script.decompose()

        clean_text = soup.get_text(separator=" ")
        emails = set()

        for e in STANDARD_EMAIL_REGEX.findall(clean_text):
            e_clean = e.lower().strip()
            tld = e_clean.split(".")[-1]
            if tld not in INVALID_TLDS and not any(p in e_clean for p in ["window.", "location", "dataLayer"]):
                emails.add(e_clean)

        return emails

    async def search_email_candidates(
        self, session: aiohttp.ClientSession, name: str, company: str
    ) -> Set[str]:
        """Searches web indexes for target contact email candidates."""
        candidates: Set[str] = set()
        clean_comp = sanitize_company_name(company)
        clean_name = re.sub(r"[^a-zA-Z\s]", "", str(name)).strip()

        if not clean_comp or is_self_employed(company):
            return set()

        query = f'"{clean_name}" "{clean_comp}" email'
        search_url = "https://html.duckduckgo.com/html/"
        post_data = {"q": query}
        post_headers = {
            **HEADERS,
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://html.duckduckgo.com",
            "Referer": "https://html.duckduckgo.com/",
        }

        try:
            async with session.post(search_url, data=post_data, headers=post_headers, timeout=self.timeout) as resp:
                if resp.status == 200:
                    html_text = await resp.text()
                    candidates.update(self.extract_clean_emails(html_text))
        except Exception:
            pass

        return candidates

    async def process_email(
        self, session: aiohttp.ClientSession, email: str, name: str, company: str
    ) -> Tuple[Optional[str], str]:
        """Executes full Email Verification & Enrichment Flow."""
        if is_self_employed(company):
            return None, "SELF_EMPLOYED_NO_DOMAIN"

        # 1. Test Existing Email Address
        if email and str(email).lower() != "nan" and "@" in str(email):
            orig_domain = str(email).split("@")[-1].strip().lower()
            mx_hosts = await self.get_mx_records(orig_domain)

            if mx_hosts:
                smtp_ok = await self.verify_smtp_mailbox(email, mx_hosts[0])
                if smtp_ok is True:
                    return email, "SMTP_HANDSHAKE_VERIFIED"
                return email, "MX_VERIFIED"

        # 2. Fallback Search Engine Extraction + Strict Gate Verification
        candidates = await self.search_email_candidates(session, name, company)
        for candidate in candidates:
            cand_domain = candidate.split("@")[-1].strip().lower()
            mx_hosts = await self.get_mx_records(cand_domain)

            if mx_hosts:
                smtp_ok = await self.verify_smtp_mailbox(candidate, mx_hosts[0])
                if smtp_ok is True:
                    return candidate, "SEARCH_SMTP_VERIFIED"
                elif smtp_ok is None:
                    clean_comp = re.sub(r"[^a-zA-Z0-9]", "", company).lower()
                    if clean_comp and clean_comp in cand_domain.replace(".", ""):
                        return candidate, "SEARCH_SCRAPED_MX_VERIFIED"

        return None, "UNRESOLVED"

    # --- LINKEDIN RESOLUTION ENGINE ---

    async def verify_url_live(self, session: aiohttp.ClientSession, url: str) -> bool:
        """Sends HTTP GET request to verify live 200 OK response on LinkedIn."""
        if not url or "linkedin.com" not in url:
            return False
        try:
            async with session.get(url, headers=HEADERS, timeout=self.timeout, allow_redirects=True) as resp:
                if resp.status == 200 and "404" not in str(resp.url):
                    return True
                return False
        except Exception:
            return False

    def build_linkedin_candidates(
        self, raw_url: str, name: str, company: str
    ) -> List[Tuple[str, str]]:
        """Generates candidate LinkedIn URLs ordered by precision."""
        candidates = []
        clean_name = clean_text_to_ascii(name)
        clean_comp = sanitize_company_name(company)
        decoded_url = clean_text_to_ascii(raw_url)

        # Candidate A: Legacy /pub/ extraction
        if "/pub/" in decoded_url:
            match = re.search(r"linkedin\.com/pub/([^/]+)", decoded_url)
            if match:
                clean_slug = re.sub(r"[^a-z0-9-]", "", match.group(1)).strip("-")
                if clean_slug and len(clean_slug) > 2:
                    candidates.append(
                        (f"https://www.linkedin.com/in/{clean_slug}", "PROFILE_RESOLVED_FROM_LEGACY_PUB")
                    )

        # Candidate B: Cleaned /in/ URL
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

        # Candidate C: Official Company Page Fallback
        if clean_comp and not is_self_employed(company):
            comp_slug = "-".join(re.sub(r"[^a-z0-9\s]", "", clean_comp).split())
            if comp_slug:
                candidates.append(
                    (f"https://www.linkedin.com/company/{comp_slug}", "COMPANY_PAGE_RESOLVED")
                )

        return candidates

    async def process_linkedin(
        self, session: aiohttp.ClientSession, row: Dict[str, Any]
    ) -> Tuple[Optional[str], str]:
        """Executes full LinkedIn URL Resolution Flow."""
        raw_url = str(row.get("linkedin_url", "")).strip()
        norm_url = str(row.get("linkedin_normalized_url", "")).strip()
        verified = str(row.get("linkedin_verified", "")).strip().lower()
        name = str(row.get("name", "")).strip()
        company = str(row.get("company", "")).strip()

        # Step 1: Keep original if marked verified valid in dataset
        if norm_url and norm_url.lower() != "nan" and verified == "yes":
            if await self.verify_url_live(session, norm_url):
                return norm_url, "LINKEDIN_ORIGINAL_VALID"

        # Step 2: Test Canonical Candidate URLs with Live HTTP Gate
        candidates = self.build_linkedin_candidates(raw_url, name, company)
        for cand_url, status in candidates:
            if await self.verify_url_live(session, cand_url):
                return cand_url, status

        return None, "LINKEDIN_UNRESOLVED"

    # --- UNIFIED ROW PROCESSOR ---

    async def process_row(
        self, row: Dict[str, Any], session: aiohttp.ClientSession
    ) -> Dict[str, Any]:
        record = dict(row)
        email = str(row.get("email", "")).strip()
        name = str(row.get("name", "")).strip()
        company = str(row.get("company", "")).strip()

        # Run Email and LinkedIn processes concurrently
        email_task = self.process_email(session, email, name, company)
        linkedin_task = self.process_linkedin(session, row)

        (final_email, email_status), (final_linkedin, linkedin_status) = await asyncio.gather(
            email_task, linkedin_task
        )

        record["final_email"] = final_email
        record["verification_status"] = email_status
        record["final_linkedin_url"] = final_linkedin
        record["linkedin_resolution_status"] = linkedin_status

        return record


async def run_pipeline(input_file: str, output_file: str, batch_size: int = 5):
    logging.info(f"Starting Ingestion on Input Dataset: {input_file}")

    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    if os.path.exists(output_file):
        os.remove(output_file)

    # Autodetect Delimiter (Comma vs Tab)
    try:
        df = pd.read_csv(input_file, sep=None, engine="python")
    except Exception:
        df = pd.read_csv(input_file)

    records = df.to_dict(orient="records")
    total_records = len(records)
    total_batches = (total_records + batch_size - 1) // batch_size
    logging.info(f"Loaded {total_records} records across {total_batches} batches.")

    pipeline = UnifiedDataPipeline()
    connector = aiohttp.TCPConnector(limit=batch_size * 2, ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, total_records, batch_size):
            batch_num = (i // batch_size) + 1
            batch = records[i : i + batch_size]
            logging.info(f"Processing Batch {batch_num} / {total_batches}...")

            tasks = [pipeline.process_row(row, session) for row in batch]
            batch_results = await asyncio.gather(*tasks)

            # Stream Batch directly to Disk
            batch_df = pd.DataFrame(batch_results)
            is_first_batch = (i == 0)
            batch_df.to_csv(
                output_file, mode="a", sep=",", index=False, header=is_first_batch
            )
            logging.info(f"Batch {batch_num} / {total_batches} successfully saved to {output_file}")

    logging.info(f"Pipeline Execution Complete! Output written to: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Batch Data Enrichment Pipeline for Email & LinkedIn.")
    parser.add_argument(
        "-i", "--input", default="raw_data/linkedin_verified_only.csv", help="Input CSV/TSV file path"
    )
    parser.add_argument(
        "-o", "--output", default="processed/enriched_output.csv", help="Output CSV file path"
    )
    parser.add_argument(
        "-b", "--batch-size", type=int, default=5, help="Batch size for concurrent processing (Default: 5)"
    )

    args = parser.parse_args()
    asyncio.run(run_pipeline(args.input, args.output, args.batch_size))


if __name__ == "__main__":
    main()