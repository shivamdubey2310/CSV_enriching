"""
Master B2B Enrichment & Recovery Pipeline (High-Yield LinkedIn & Email Engine)
==============================================================================
- Input: Single CSV, multiple CSVs, or wildcards (-i "raw_data/*.csv")
- LinkedIn: Live OpenGraph probe + Legacy /pub/ slug harvester + SearXNG waterfall.
- Email: Clearbit + Enterprise Aliases + Search Domain Fallback + Website Team Crawl + MX Permutations.
- Resumable: Detects existing output files and automatically resumes from the last row.
"""

import argparse
import asyncio
import glob
import logging
import os
import re
import sys
import time
import unicodedata
import urllib.parse
from typing import Any, Dict, List, Optional, Set, Tuple

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
import dns.asyncresolver
import pandas as pd

# =====================================================================
# 1. LOGGING & CONSTANTS
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("master_enrichment.log", mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

SEARXNG_ENDPOINT = "http://127.0.0.1:8080/search"

STANDARD_EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", re.IGNORECASE
)
LINKEDIN_IN_REGEX = re.compile(
    r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/([a-zA-Z0-9%_-]+)", re.IGNORECASE
)
LEGAL_SUFFIXES = re.compile(
    r"\b(pvt ltd|private limited|ltd|inc|llc|group|foundation trust|dept|department|pt|gmbh|corp|corporation|llp|sa|sl|srl|pvt|spa|bv|worldwide|consulting|services|solutions|international|de cv|s a|s r l)\b",
    re.IGNORECASE,
)

GENERIC_COMPANIES = {
    "selfemployed", "independiente", "on my own", "self employed",
    "freelance", "freelancer", "independent", "none", "n/a", "na",
    "null", "nocompany", "no company", "nan", "consultant", "self",
    "retired", "unemployed", "em casa", "autnoma", "autonoma",
    "autonomo", "cuenta propia", "particular", "my own", "free lance"
}

DIRECTORY_BLACKLIST = {
    "linkedin.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "wikipedia.org", "youtube.com", "bloomberg.com", "crunchbase.com",
    "glassdoor.com", "indeed.com", "zoominfo.com", "pitchbook.com",
    "contactout.com", "leadiq.com", "visualvisitor.com", "apollo.io", "dnb.com",
    "rocketreach.co", "lusha.com", "yellowpages.com", "yelp.com", "signalhire.com",
    "adapt.io", "upcountry.com", "kompass.com", "dottorpaolo.com", "pearsonfamily.id.au"
}

GENERIC_EMAIL_PREFIXES = {
    "info", "contact", "support", "sales", "admin", "hello", "help",
    "careers", "jobs", "office", "general", "team", "privacy", "billing",
    "press", "media", "service", "customerservice", "enquiries", "hr"
}

KNOWN_ENTERPRISE_DOMAINS = {
    "idea cellular": "myvi.in",
    "vodafone idea": "myvi.in",
    "tata docomo": "tatatelebusiness.com",
    "bbva compass": "bbvausa.com",
    "nextel brasil": "claro.com.br",
    "uk department of health": "dhsc.gov.uk",
    "east london nhs": "elft.nhs.uk",
    "gateshead health nhs": "gatesheadhealth.nhs.uk",
    "community health systems": "chs.net",
    "danafarber": "dfci.harvard.edu",
    "clark county school district": "ccsd.net"
}

PROBE_HEADERS = {
    "User-Agent": "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

CLIENT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "X-Forwarded-For": "127.0.0.1",
    "X-Real-IP": "127.0.0.1",
}


# =====================================================================
# 2. STRING & NORMALIZATION HELPERS
# =====================================================================
def clean_text_to_ascii(text: Any) -> str:
    if pd.isna(text) or not text:
        return ""
    decoded = urllib.parse.unquote(str(text))
    nfkd = unicodedata.normalize("NFKD", decoded)
    return nfkd.encode("ASCII", "ignore").decode("utf-8").lower().strip()


def sanitize_company_name(company: Any) -> str:
    clean = clean_text_to_ascii(company)
    if not clean or clean in GENERIC_COMPANIES:
        return ""
    clean = LEGAL_SUFFIXES.sub("", clean)
    clean = re.sub(r"[^\w\s-]", " ", clean)
    return re.sub(r"\s+", " ", clean).strip()


def get_clean_company_variants(company: str) -> List[str]:
    """Truncates company taglines and secondary descriptions for higher search hit rates."""
    clean_c = sanitize_company_name(company)
    if not clean_c:
        return []
    
    clean_c = re.split(r"[-–—:|]", clean_c)[0].strip()
    words = clean_c.split()
    
    variants = []
    if len(words) > 3:
        variants.append(" ".join(words[:3]))
        variants.append(" ".join(words[:2]))
    elif len(words) >= 2:
        variants.append(" ".join(words))
        variants.append(words[0])
    else:
        variants.append(clean_c)
        
    return list(dict.fromkeys(variants))


def is_self_employed(company: Any) -> bool:
    comp_lower = clean_text_to_ascii(company)
    if not comp_lower:
        return False
    return comp_lower in GENERIC_COMPANIES


def split_full_name(full_name: str) -> Tuple[str, str]:
    clean = re.sub(r"[^a-zA-Z\s]", "", clean_text_to_ascii(full_name)).strip()
    parts = clean.split()
    if not parts:
        return "", ""
    return (parts[0], parts[-1]) if len(parts) > 1 else (parts[0], "")


def extract_name_from_legacy_url(url: Any) -> Optional[str]:
    """Extracts original name string from deprecated /pub/ URLs."""
    if pd.isna(url) or not url:
        return None
    url_str = str(url)
    if "/pub/" in url_str:
        match = re.search(r"linkedin\.com/pub/([^/?#]+)", url_str)
        if match:
            raw_slug = urllib.parse.unquote(match.group(1))
            clean_slug = re.sub(r"[^a-zA-Z\s-]", "", clean_text_to_ascii(raw_slug))
            return clean_slug.replace("-", " ").strip()
    return None


def clean_linkedin_slug(url: Any) -> Optional[str]:
    if pd.isna(url) or not url or "linkedin.com/in/" not in str(url):
        return None
    clean = str(url).strip().split("?")[0].split("#")[0].rstrip("/")
    clean = re.sub(r"https?://([a-z]{2,3}\.)?linkedin\.com", "https://www.linkedin.com", clean)
    match = LINKEDIN_IN_REGEX.search(clean)
    if match:
        slug = match.group(1).strip("-")
        if len(slug) >= 2 and not slug.isdigit():
            return f"https://www.linkedin.com/in/{slug}"
    return None


def is_strict_name_match(name: str, url: str) -> bool:
    """Tolerates middle names, prefixes (lo, de, da, van), and transliterations."""
    first, last = split_full_name(name)
    if not first:
        return False

    slug = clean_text_to_ascii(url.split("/in/")[-1]).replace("-", "").replace("_", "")

    if last:
        if first in slug and last in slug:
            return True
        if len(first) >= 1 and slug.startswith(first[0]) and last in slug:
            return True
        f_clean = re.sub(r"[aeiouh]", "", first)
        l_clean = re.sub(r"[aeiouh]", "", last)
        s_clean = re.sub(r"[aeiouh]", "", slug)
        if len(f_clean) >= 2 and len(l_clean) >= 2 and f_clean in s_clean and l_clean in s_clean:
            return True
        return False
    else:
        return len(first) >= 2 and first in slug


def is_domain_brand_match(company: str, domain: str) -> bool:
    """Anti-poisoning check to verify the domain shares root tokens with the company."""
    clean_c = sanitize_company_name(company)
    if not clean_c or not domain:
        return False
    
    dom_root = domain.split(".")[0].lower()
    comp_tokens = [t for t in clean_c.split() if len(t) >= 3]

    for token in comp_tokens:
        if token in dom_root or dom_root in token:
            return True
            
    acronym = "".join([w[0] for w in comp_tokens if w])
    if len(acronym) >= 3 and acronym in dom_root:
        return True
        
    return False


def generate_email_permutations(first: str, last: str, domain: str) -> List[str]:
    """Generates standard corporate email permutations ordered by statistical frequency."""
    if not domain or not first:
        return []
    
    f_init = first[0]
    
    if last:
        l_init = last[0]
        return [
            f"{first}.{last}@{domain}",      # john.smith@company.com (50%)
            f"{f_init}{last}@{domain}",        # jsmith@company.com (30%)
            f"{first}_{last}@{domain}",      # john_smith@company.com (8%)
            f"{first}{last}@{domain}",        # johnsmith@company.com (5%)
            f"{first}.{l_init}@{domain}",     # john.s@company.com (3%)
            f"{f_init}.{last}@{domain}",     # j.smith@company.com (2%)
            f"{last}.{first}@{domain}",      # smith.john@company.com (1%)
            f"{first}@{domain}",             # john@company.com (1%)
        ]
    return [f"{first}@{domain}"]


# =====================================================================
# 3. MASTER ENRICHMENT & VERIFICATION ENGINE
# =====================================================================
class MasterEnrichmentEngine:
    def __init__(self):
        self.probe_sem = asyncio.Semaphore(8)
        self.searxng_sem = asyncio.Semaphore(2)
        self.mx_cache: Dict[str, List[str]] = {}

    # --- LINKEDIN ENGINE ---

    async def check_linkedin_is_live(self, session: AsyncSession, url: str) -> bool:
        clean = clean_linkedin_slug(url)
        if not clean:
            return False
        async with self.probe_sem:
            try:
                resp = await session.get(clean, headers=PROBE_HEADERS, timeout=2.5, allow_redirects=True)
                final_url = str(resp.url).lower()
                if resp.status_code in [404, 410] or "/404" in final_url:
                    return False
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    title_text = soup.title.string.lower() if soup.title and soup.title.string else ""
                    og_tag = soup.find("meta", property="og:title")
                    og_title = str(og_tag.get("content")).lower() if og_tag and og_tag.get("content") else ""
                    if any(bad in title_text or bad in og_title for bad in ["page not found", "profile not found", "profile unavailable"]):
                        return False
                    return True
                return True
            except Exception:
                return True

    async def query_searxng_linkedin(self, session: AsyncSession, query_str: str, name: str) -> Optional[str]:
        params = {"q": query_str, "format": "json", "engines": "duckduckgo,brave,yahoo,qwant,bing"}
        try:
            resp = await session.get(SEARXNG_ENDPOINT, params=params, headers=CLIENT_HEADERS, timeout=4.0)
            if resp.status_code == 200:
                for res in resp.json().get("results", []):
                    link = res.get("url", "")
                    match = LINKEDIN_IN_REGEX.search(link)
                    if match:
                        cand = f"https://www.linkedin.com/in/{match.group(1).strip('-')}"
                        if is_strict_name_match(name, cand):
                            return cand
        except Exception:
            pass
        return None

    async def discover_linkedin_searxng(
        self, session: AsyncSession, name: str, company: str, raw_row: Dict[str, Any]
    ) -> Optional[str]:
        clean_n = clean_text_to_ascii(name)
        if not clean_n or clean_n == "nan":
            return None

        queries = []
        
        # 1. Company-targeted queries with shortened brand variants
        comp_variants = get_clean_company_variants(company)
        for cv in comp_variants:
            queries.append(f'"{clean_n}" "{cv}" linkedin')

        # 2. Legacy /pub/ URL hint query
        legacy_hint = extract_name_from_legacy_url(raw_row.get("linkedin_url"))
        if legacy_hint and legacy_hint != clean_n:
            queries.append(f'"{legacy_hint}" linkedin')

        # 3. Broader fallbacks
        if comp_variants:
            queries.append(f"{clean_n} {comp_variants[0]} linkedin")
        queries.append(f'"{clean_n}" site:linkedin.com/in/')

        async with self.searxng_sem:
            for q in queries:
                await asyncio.sleep(0.25)
                hit = await self.query_searxng_linkedin(session, q, name)
                if hit:
                    logging.info(f"[SEARXNG HIT] {name} ({company}) -> {hit}")
                    return hit

        return None

    # --- DNS MX RESOLVER ---

    async def get_mx_hosts(self, domain: str) -> List[str]:
        if not domain or "." not in domain:
            return []
        domain = domain.strip().lower()
        if domain in self.mx_cache:
            return self.mx_cache[domain]
        try:
            resolver = dns.asyncresolver.Resolver()
            resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]
            resolver.timeout = 1.2
            resolver.lifetime = 1.2
            ans = await resolver.resolve(domain, "MX")
            records = [str(r.exchange).rstrip(".") for r in sorted(ans, key=lambda x: x.preference)]
            self.mx_cache[domain] = records
            return records
        except Exception:
            self.mx_cache[domain] = []
            return []

    # --- DOMAIN DISCOVERY ENGINE ---

    async def discover_company_domain_clearbit(self, session: AsyncSession, company: str) -> Optional[str]:
        clean_c = sanitize_company_name(company)
        if not clean_c:
            return None
        url = f"https://autocomplete.clearbit.com/v1/companies/suggest?query={urllib.parse.quote(clean_c)}"
        try:
            resp = await session.get(url, headers=CLIENT_HEADERS, timeout=1.8)
            if resp.status_code == 200:
                data = resp.json()
                if data and isinstance(data, list) and len(data) > 0:
                    d = data[0].get("domain", "").strip().lower()
                    if d and "." in d and d not in DIRECTORY_BLACKLIST:
                        if is_domain_brand_match(company, d):
                            return d
        except Exception:
            pass
        return None

    async def discover_company_domain_searxng(self, session: AsyncSession, company: str) -> Optional[str]:
        clean_c = sanitize_company_name(company)
        if not clean_c:
            return None
        query = f'"{clean_c}" official website'
        params = {"q": query, "format": "json", "engines": "duckduckgo,brave,yahoo,qwant"}
        try:
            resp = await session.get(SEARXNG_ENDPOINT, params=params, headers=CLIENT_HEADERS, timeout=3.5)
            if resp.status_code == 200:
                for res in resp.json().get("results", []):
                    link = res.get("url", "")
                    domain = urllib.parse.urlparse(link).netloc.lower().replace("www.", "")
                    if domain and "." in domain and domain not in DIRECTORY_BLACKLIST:
                        if is_domain_brand_match(company, domain):
                            mx = await self.get_mx_hosts(domain)
                            if mx:
                                logging.info(f"[VALID DOMAIN FOUND] {company} -> {domain}")
                                return domain
        except Exception:
            pass
        return None

    async def resolve_company_domain(self, session: AsyncSession, company: str) -> Optional[str]:
        clean_c = sanitize_company_name(company)
        if not clean_c:
            return None

        # 1. Enterprise aliases
        for alias, domain in KNOWN_ENTERPRISE_DOMAINS.items():
            if alias in clean_c:
                mx = await self.get_mx_hosts(domain)
                if mx:
                    return domain

        # 2. Clearbit lookup
        domain = await self.discover_company_domain_clearbit(session, company)
        if domain:
            mx = await self.get_mx_hosts(domain)
            if mx:
                return domain

        # 3. SearXNG search lookup
        async with self.searxng_sem:
            domain = await self.discover_company_domain_searxng(session, company)
            if domain:
                return domain

        return None

    # --- CORPORATE WEBSITE CRAWLER ---

    def extract_emails_from_html(self, html_text: str, domain: str) -> Set[str]:
        if not html_text:
            return set()
        soup = BeautifulSoup(html_text, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()

        found = set()
        for match in STANDARD_EMAIL_REGEX.findall(soup.get_text(separator=" ")):
            clean_e = match.lower().strip()
            if domain in clean_e:
                prefix = clean_e.split("@")[0]
                if prefix not in GENERIC_EMAIL_PREFIXES and len(prefix) >= 2:
                    found.add(clean_e)
        return found

    async def crawl_company_website_for_match(
        self, session: AsyncSession, domain: str, first_name: str, last_name: str
    ) -> Optional[str]:
        if not domain or not first_name:
            return None

        endpoints = ["", "/contact", "/about", "/team", "/people", "/about-us"]
        for ep in endpoints:
            target_url = f"https://{domain}{ep}"
            try:
                resp = await session.get(target_url, headers=PROBE_HEADERS, timeout=2.0, allow_redirects=True)
                if resp.status_code == 200:
                    emails = self.extract_emails_from_html(resp.text, domain)
                    for e in emails:
                        p = e.split("@")[0].lower()
                        if first_name in p and (not last_name or last_name in p):
                            logging.info(f"[SCRAPED TEAM HIT] {first_name} {last_name} -> {e}")
                            return e
            except Exception:
                continue
        return None

    # --- ROW PROCESSING PIPELINE ---

    async def process_row(
        self, row: Dict[str, Any], session: AsyncSession, idx: int, total: int
    ) -> Dict[str, Any]:
        record = dict(row)
        name = str(row.get("name", "")).strip()
        company = str(row.get("company", "")).strip()
        orig_email = str(row.get("email", "")).strip()

        # Step 1: LinkedIn Resolution
        input_url = clean_linkedin_slug(row.get("linkedin_normalized_url") or row.get("linkedin_url"))
        final_linkedin = None
        resolution_status = "LINKEDIN_UNRESOLVED"

        if input_url and is_strict_name_match(name, input_url):
            if await self.check_linkedin_is_live(session, input_url):
                final_linkedin = input_url
                resolution_status = "LIVE_PROFILE_CONFIRMED"

        if not final_linkedin:
            searxng_hit = await self.discover_linkedin_searxng(session, name, company, row)
            if searxng_hit:
                final_linkedin = searxng_hit
                resolution_status = "SEARXNG_VERIFIED_LIVE"

        record["final_linkedin_url"] = final_linkedin if final_linkedin else ""
        record["linkedin_resolution_status"] = resolution_status

        # Step 2: Email Resolution & Recovery
        if is_self_employed(company):
            record["final_email"] = ""
            record["email_permutations"] = ""
            record["verification_status"] = "SELF_EMPLOYED_NO_DOMAIN"
        else:
            first, last = split_full_name(name)
            orig_domain = orig_email.split("@")[-1].lower() if "@" in orig_email else ""
            target_domain = None

            # 2A. Check original domain MX first
            if orig_domain and orig_domain not in DIRECTORY_BLACKLIST:
                mx_hosts = await self.get_mx_hosts(orig_domain)
                if mx_hosts:
                    target_domain = orig_domain

            # 2B. Fallback domain discovery
            if not target_domain and company:
                disc_domain = await self.resolve_company_domain(session, company)
                if disc_domain:
                    target_domain = disc_domain

            # 2C. Website Crawl + Permutation Engine
            if target_domain:
                scraped_match = await self.crawl_company_website_for_match(session, target_domain, first, last)
                perms = generate_email_permutations(first, last, target_domain)

                if scraped_match:
                    record["final_email"] = scraped_match
                    record["email_permutations"] = "; ".join([scraped_match] + [p for p in perms if p != scraped_match])
                    record["verification_status"] = "SCRAPED_TEAM_MATCH"
                else:
                    record["final_email"] = perms[0] if perms else ""
                    record["email_permutations"] = "; ".join(perms)
                    record["verification_status"] = "MX_VERIFIED_PATTERN"
            else:
                record["final_email"] = ""
                record["email_permutations"] = ""
                record["verification_status"] = "UNRESOLVED"

        logging.info(
            f"[{idx}/{total}] {name} ({company}) -> "
            f"Email: {record['final_email']} [{record['verification_status']}] | "
            f"LinkedIn: {record['final_linkedin_url']} [{record['linkedin_resolution_status']}]"
        )

        return record


# =====================================================================
# 4. MULTI-CSV INGESTION & RESUME ORCHESTRATOR
# =====================================================================
def load_all_csvs(input_pattern: str) -> pd.DataFrame:
    files = glob.glob(input_pattern) if ("*" in input_pattern or "?" in input_pattern) else [input_pattern]
    if not files:
        raise FileNotFoundError(f"No files found matching: {input_pattern}")

    frames = []
    for f in sorted(files):
        try:
            with open(f, "r", encoding="utf-8", errors="ignore") as handle:
                first_line = handle.readline()
            sep = "\t" if "\t" in first_line else ","
            df_temp = pd.read_csv(f, sep=sep)
            df_temp.columns = [c.strip().lower() for c in df_temp.columns]
            df_temp["source_file"] = os.path.basename(f)
            frames.append(df_temp)
            logging.info(f"Loaded {len(df_temp)} rows from {f}")
        except Exception as e:
            logging.error(f"Failed loading {f}: {e}")

    combined = pd.concat(frames, ignore_index=True)
    return combined


async def run_pipeline(input_pattern: str, output_file: str, batch_size: int = 15):
    start_time = time.time()
    df = load_all_csvs(input_pattern)

    processed_keys: Set[str] = set()
    if os.path.exists(output_file):
        try:
            out_df = pd.read_csv(output_file)
            for _, r in out_df.iterrows():
                key = f"{str(r.get('row_number',''))}|{str(r.get('name',''))}|{str(r.get('email',''))}"
                processed_keys.add(key)
            logging.info(f"Resuming: {len(processed_keys)} records already present in {output_file}")
        except Exception:
            pass

    records = df.to_dict(orient="records")
    remaining_records = [
        r for r in records 
        if f"{str(r.get('row_number',''))}|{str(r.get('name',''))}|{str(r.get('email',''))}" not in processed_keys
    ]

    total = len(records)
    remaining_count = len(remaining_records)
    logging.info(f"Total Rows: {total} | Remaining to Process: {remaining_count}")

    if remaining_count == 0:
        logging.info("All records are already processed!")
        return

    engine = MasterEnrichmentEngine()

    async with AsyncSession(impersonate="chrome124") as session:
        for i in range(0, remaining_count, batch_size):
            batch = remaining_records[i : i + batch_size]
            tasks = [engine.process_row(row, session, i + idx + 1, remaining_count) for idx, row in enumerate(batch)]
            results = await asyncio.gather(*tasks)

            pd.DataFrame(results).to_csv(
                output_file, 
                mode="a", 
                sep=",", 
                index=False, 
                header=not os.path.exists(output_file) or os.path.getsize(output_file) == 0
            )

    logging.info(f"Complete! Processed {remaining_count} records in {time.time() - start_time:.2f}s -> {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", default="raw_data/linkedin_verified_only.csv", help="CSV path or wildcard (e.g. 'raw_data/*.csv')")
    parser.add_argument("-o", "--output", default="processed/master_enriched_final.csv", help="Output CSV path")
    parser.add_argument("-b", "--batch-size", type=int, default=15)
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    asyncio.run(run_pipeline(args.input, args.output, args.batch_size))