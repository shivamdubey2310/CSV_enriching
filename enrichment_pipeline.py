import asyncio
import logging
import os
import re
import urllib.parse
from typing import Dict, Any, Optional, Set, List
from urllib.parse import urlparse

import aiohttp
import aiosmtplib
import dns.asyncresolver
from bs4 import BeautifulSoup
import pandas as pd

# Production Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("email_verification_search.log"),
        logging.StreamHandler(),
    ],
)

STANDARD_EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", re.IGNORECASE
)

LEGAL_SUFFIXES = re.compile(
    r"\b(pvt ltd|private limited|ltd|inc|llc|group|foundation trust|dept|department|pt|gmbh|corp|corporation|llp|sa|sl|srl|pvt)\b",
    re.IGNORECASE,
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

INVALID_TLDS = {
    "href", "push", "our", "png", "jpg", "jpeg", "svg", "gif", 
    "webp", "css", "js", "json", "html", "php", "aspx", "ts"
}


def sanitize_company_name(company: str) -> str:
    """Cleans legal suffixes and formatting noise from company names."""
    if not company or pd.isna(company):
        return ""
    clean = str(company)
    clean = re.sub(r"embeeded", "embedded", clean, flags=re.IGNORECASE)
    clean = LEGAL_SUFFIXES.sub("", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def is_self_employed(company: str) -> bool:
    """Identifies freelancers or self-employed individuals."""
    if not company or pd.isna(company):
        return True
    comp_lower = str(company).lower().strip()
    return comp_lower in {
        "selfemployed", "independiente", "on my own", 
        "self employed", "freelance", "freelancer", "independent"
    }


class SearchAndVerifyEmailPipeline:
    def __init__(self, timeout_seconds: int = 5):
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._mx_cache: Dict[str, List[str]] = {}

    def _get_resolver(self) -> dns.asyncresolver.Resolver:
        resolver = dns.asyncresolver.Resolver()
        resolver.nameservers = ["8.8.8.8", "1.1.1.1"]
        resolver.lifetime = 3.0
        return resolver

    async def get_mx_records(self, domain: str) -> List[str]:
        """Resolves active MX records for a domain."""
        if not domain or pd.isna(domain) or "." not in str(domain):
            return []

        clean_domain = str(domain).strip().lower()
        if clean_domain in self._mx_cache:
            return self._mx_cache[clean_domain]

        try:
            resolver = self._get_resolver()
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
        """Performs asynchronous SMTP handshake (RCPT TO) for strict validation."""
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

    async def search_and_extract_candidates(
        self, session: aiohttp.ClientSession, name: str, company: str
    ) -> Set[str]:
        """Searches DuckDuckGo for the person + company and extracts raw candidate emails."""
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
                    # 1. Extract from search result snippets directly
                    snippet_emails = self.extract_clean_emails(html_text)
                    candidates.update(snippet_emails)

                    # 2. Fetch top 2 organic result URLs
                    soup = BeautifulSoup(html_text, "html.parser")
                    links = soup.find_all("a", class_="result__url")[:2]

                    for link in links:
                        raw_href = link.get("href", "")
                        actual_url = (
                            urllib.parse.unquote(raw_href.split("uddg=")[1].split("&")[0])
                            if "uddg=" in raw_href
                            else raw_href
                        )
                        if actual_url.startswith("http") and not any(b in actual_url for b in ["linkedin.com", "duckduckgo.com", "facebook.com"]):
                            try:
                                async with session.get(actual_url, headers=HEADERS, timeout=self.timeout, ssl=False) as page_resp:
                                    if page_resp.status == 200:
                                        page_text = await page_resp.text()
                                        candidates.update(self.extract_clean_emails(page_text))
                            except Exception:
                                continue
        except Exception:
            pass

        return candidates

    async def process_row(self, row: Dict[str, Any], session: aiohttp.ClientSession) -> Dict[str, Any]:
        record = dict(row)
        email = str(row.get("email", "")).strip()
        name = str(row.get("name", "")).strip()
        company = str(row.get("company", "")).strip()

        # Step 0: Filter self-employed
        if is_self_employed(company):
            record["final_email"] = None
            record["verification_status"] = "SELF_EMPLOYED_NO_DOMAIN"
            return record

        # Step 1: Check existing email (DNS MX + SMTP Handshake)
        if email and email.lower() != "nan" and "@" in email:
            original_domain = email.split("@")[-1].strip().lower()
            mx_hosts = await self.get_mx_records(original_domain)

            if mx_hosts:
                smtp_result = await self.verify_smtp_mailbox(email, mx_hosts[0])
                if smtp_result is True:
                    record["final_email"] = email
                    record["verification_status"] = "SMTP_HANDSHAKE_VERIFIED"
                    return record
                else:
                    record["final_email"] = email
                    record["verification_status"] = "MX_VERIFIED"
                    return record

        logging.info(f"MX failed for {email}. Executing search extraction for {name} ({company})...")

        # Step 2: Search engine candidate extraction
        candidates = await self.search_and_extract_candidates(session, name, company)

        # Step 3: STRICT VERIFICATION GATE for search candidates
        for candidate in candidates:
            cand_domain = candidate.split("@")[-1].strip().lower()
            mx_hosts = await self.get_mx_records(cand_domain)

            if mx_hosts:
                # Run direct SMTP handshake test
                smtp_ok = await self.verify_smtp_mailbox(candidate, mx_hosts[0])
                if smtp_ok is True:
                    record["final_email"] = candidate
                    record["verification_status"] = "SEARCH_SMTP_VERIFIED"
                    return record
                elif smtp_ok is None:
                    # If Port 25 is inconclusive/blocked, accept ONLY if candidate domain matches company
                    clean_comp = re.sub(r"[^a-zA-Z0-9]", "", company).lower()
                    if clean_comp and clean_comp in cand_domain.replace(".", ""):
                        record["final_email"] = candidate
                        record["verification_status"] = "SEARCH_SCRAPED_MX_VERIFIED"
                        return record

        # Strict Fallback: Leave blank if no candidate passed verification
        record["final_email"] = None
        record["verification_status"] = "UNRESOLVED"
        return record


async def run_pipeline(input_file: str, output_file: str, batch_size: int = 5):
    logging.info(f"Starting Search & Strict Verification Pipeline on: {input_file}")

    if os.path.exists(output_file):
        os.remove(output_file)

    try:
        df = pd.read_csv(input_file, sep="\t")
    except Exception:
        df = pd.read_csv(input_file)

    records = df.to_dict(orient="records")
    pipeline = SearchAndVerifyEmailPipeline()
    total_batches = (len(records) + batch_size - 1) // batch_size

    connector = aiohttp.TCPConnector(limit=batch_size, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(records), batch_size):
            batch_num = (i // batch_size) + 1
            batch = records[i : i + batch_size]
            logging.info(f"Processing batch {batch_num} / {total_batches}...")

            tasks = [pipeline.process_row(row, session) for row in batch]
            batch_results = await asyncio.gather(*tasks)

            # Append batch directly to disk
            batch_df = pd.DataFrame(batch_results)
            is_first_batch = (i == 0)
            batch_df.to_csv(
                output_file, mode="a", sep=",", index=False, header=is_first_batch
            )
            logging.info(f"Batch {batch_num} saved to {output_file}")

    logging.info(f"Pipeline complete! Output saved to: {output_file}")


if __name__ == "__main__":
    INPUT_PATH = "raw_data/linkedin_verified_only.csv"
    OUTPUT_PATH = "processed/linkedin_verified_only.csv"

    asyncio.run(run_pipeline(INPUT_PATH, OUTPUT_PATH, batch_size=5))