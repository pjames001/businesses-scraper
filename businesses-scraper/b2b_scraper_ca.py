import os
import re
import json
import csv
import glob
import argparse
import asyncio
import multiprocessing
import aiohttp
try:
    import aiodns  # async DNS: lookups don't tie up threads
except ImportError:
    aiodns = None
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from html import unescape
from urllib.parse import urljoin, urlsplit

# Paths are relative to this script, so the run works from any folder
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Input dataset path relative to businesses-scraper directory
input_file = os.path.join(SCRIPT_DIR, "..", "free_company_dataset.json")

today_str = date.today().strftime("%Y-%m-%d")
output_dir = os.path.join(SCRIPT_DIR, "..", "filtered_b2b_california_scraped")
os.makedirs(output_dir, exist_ok=True)

# Results from earlier single-process runs; read first, then overridden by the worker checkpoints
LEGACY_CHECKPOINT = "checkpoint_scraped_contacts.jsonl"
# Checkpoint file prefixes: full runs and --retry-unreached runs keep separate checkpoints
FULL_RUN_PREFIX = "ckpt"
RETRY_RUN_PREFIX = "retryckpt"

# CPU and network settings: leave two cores free for the system, and share the connections between workers
WORKERS = max(1, min(6, (os.cpu_count() or 4) - 2))
TOTAL_CONNECTIONS = 150  # fewer simultaneous lookups, so the local DNS service isn't overwhelmed
# Query public resolvers directly instead of the local systemd-resolved stub (127.0.0.53)
DNS_SERVERS = ["1.1.1.1", "8.8.8.8"]
CONNECTIONS_PER_WORKER = max(20, TOTAL_CONNECTIONS // WORKERS)

# CSV field header structure
fieldnames = [
    "ID", "Name", "Matched Category", "Original Industry", "Country", "Region",
    "Locality", "Website", "Scraped Email", "Scraped Phone", "Founded", "Size", "Linkedin URL"
]

CATEGORY_PATTERNS = {
    "security": r"\b(security|security systems|security guards|physical security|cybersecurity|alarm systems|surveillance|private investigation|loss prevention|access control|security services)\b",
    "automotive": r"\b(auto|automotive|car dealer|vehicle|motor|dealership|auto repair)\b",
    "medical_supplies_equipment": r"\b(medical supply|medical equipment|dme|medtech|biomedical equipment|surgical supply|healthcare equipment)\b",
    "restaurant_supplies": r"\b(restaurant supply|foodservice equipment|commercial kitchen|food equipment|hospitality supply)\b",
    "wholesale": r"\b(wholesale|wholesaler|distributor|distribution|merchant wholesalers|import export|b2b trade)\b",
    "moving_companies": r"\b(moving|movers|relocation|moving and storage|van line|freight mover)\b",
    "logistics": r"\b(logistics|supply chain|freight|freight forwarding|3pl|transportation management|dispatch)\b",
    "trucking": r"\b(trucking|motor freight|haulage|hauling|drayage|carrier|truck transport)\b",
    "employment_recruitment": r"\b(recruitment|recruiting|headhunting|talent acquisition|executive search|employment agency|job placement)\b",
    "staffing": r"\b(staffing|temp agency|temporary staffing|workforce solutions|staffing agency)\b",
    "plumbing": r"\b(plumbing|plumber|pipefitting|pipe fitter|drainage|sewer service)\b",
    "law_firms": r"\b(law firm|legal services|attorney|lawyer|legal practice|legal consultancy|counsel)\b",
    "solar_systems": r"\b(solar|photovoltaic|solar energy|solar power|clean energy installation|renewable energy contracting)\b",
    "financial_accounting": r"\b(accounting|bookkeeping|cpa|financial services|tax preparation|audit|tax services|wealth management|payroll)\b",
    "pool_services": r"\b(pool service|pool maintenance|pool construction|swimming pool|pool repair)\b",
    "it_and_technology": r"\b(it services|technology|software|information technology|tech consulting|msp|cloud services|network infrastructure)\b",
    "dental_lab": r"\b(dental lab|dental laboratory|prosthodontics lab|dental prosthetics)\b",
    "warehouses": r"\b(warehouse|warehousing|cold storage|fulfillment|storage facility|logistics center)\b",
    "aerospace_defense": r"\b(aerospace|defense|aviation|defense contractor|avionics|military equipment)\b",
    "debt_collection": r"\b(debt collection|collection agency|accounts receivable management|receivables|credit collection)\b",
    "construction": r"\b(construction|general contractor|building contractor|civil engineering|heavy construction|commercial construction|remodeling)\b",
    "cleaning_services": r"\b(janitorial|commercial cleaning|building maintenance|cleaning services|facility services|pressure washing)\b",
    "handymen": r"\b(handyman|home repair|property maintenance|facilities repair|general repair)\b",
    "real_estate": r"\b(real estate|commercial real estate|property management|realty|real estate development|brokerage)\b",
    "pest_control": r"\b(pest control|exterminator|exterminating|pest management|fumigation)\b",
    "hvac": r"\b(hvac|heating|cooling|air conditioning|refrigeration|climate control|heating and air)\b",
    "landscaping": r"\b(landscap\w*|lawn care|lawn service|tree service|tree trimming|tree removal|hardscap\w*|gardening|garden service|snow removal|irrigation)\b",
    "mold_inspection": r"\b(mold|mould|mold inspection|mold remediation|mold testing|mold removal|indoor air quality|environmental testing|asbestos)\b",
    "diesel_services": r"\b(diesel|gas station|fuel station|fuel delivery|fueling|service station|truck stop|petroleum|fuel service)\b",
    "repair_maintenance": r"\b(repair|repairs|repair service|repair shop|maintenance|maintenance service|equipment repair|appliance repair|machine repair)\b",
    "building_materials": r"\b(building materials|building supply|building products|lumber|lumber yard|hardware store|concrete supply|roofing supply|construction supply)\b",
    "junk_removal": r"\b(junk removal|junk hauling|rubbish removal|waste removal|debris removal|trash removal|dumpster|dumpster rental|hauling service)\b",
    "window_installation": r"\b(window installation|window replacement|window repair|window cleaning|door installation|glass installation|glazing|windows and doors)\b",
    "electrical_contractors": r"\b(electrical|electrician|electrical contractor|electric contractor|electrical services|electrical installation|electrical service)\b",
    "investigation_services": r"\b(investigation|investigations|investigator|investigative|detective|background check|background screening)\b",
    "consulting_services": r"\b(consulting|consultant|consultants|consultancy|management consulting|business consulting|advisory services)\b",
    "storage_services": r"\b(self storage|self-storage|storage units|mini storage|storage services|storage containers|portable storage|storage solutions)\b",
}

GOVT_PATTERN = re.compile(
    r"\b(government|gov|state of|city of|county of|department of|ministry|municipality|public safety|embassy|consulate|federal agency|public school|armed forces)\b",
    re.IGNORECASE
)

EXCLUDED_B2C_PATTERN = re.compile(
    r"\b(barber|hair salon|nail salon|spa|massage|pet grooming|daycare|nursery|beauty salon|gym|fitness studio|apparel retail|shoe store|thrift store)\b",
    re.IGNORECASE
)

EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', re.IGNORECASE)
# Formatted numbers only (digits-only matches picked up IDs and timestamps); tel: links are handled separately
PHONE_REGEX = re.compile(r'(?<![\w.])(?:\+?1[-. ]?)?\(?[2-9]\d{2}(?:\)[-. ]?|[-. ])[2-9]\d{2}[-. ]\d{4}(?!\w)')
TEL_LINK_REGEX = re.compile(r'href\s*=\s*["\']tel:([^"\']+)', re.IGNORECASE)
SCRIPT_STYLE_REGEX = re.compile(r'<(script|style)\b.*?</\1\s*>', re.IGNORECASE | re.DOTALL)
PAGE_LINK_REGEX = re.compile(r'<a\b[^>]*?href\s*=\s*["\']([^"\'#]+)', re.IGNORECASE)
EXCLUDED_EMAIL_EXTS = ('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.css', '.js')

# Contact info usually lives on a sub-page, not the homepage
CONTACT_PAGE_HINTS = ("contact", "about", "reach", "location")
FALLBACK_CONTACT_PATHS = ("/contact", "/contact-us", "/about", "/about-us")
MAX_EXTRA_PAGES = 3
MAX_PAGE_BYTES = 1_500_000  # pages are read in part, which keeps memory bounded
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=8, connect=4)

def is_california(region_str):
    if not region_str:
        return False
    r = region_str.strip().lower()
    return r in ["ca", "california", "cal", "calif", "calif."]

def classify_industry(industry_str, name_str):
    combined_text = f"{industry_str or ''} {name_str or ''}".strip().lower()
    if not combined_text:
        return None
    if GOVT_PATTERN.search(combined_text) or EXCLUDED_B2C_PATTERN.search(combined_text):
        return None
    for cat_name, pattern in CATEGORY_PATTERNS.items():
        if re.search(pattern, combined_text, re.IGNORECASE):
            return cat_name
    return None

def sanitize_filename(name):
    return re.sub(r'[-\s]+', '_', name.strip().lower())

# Placeholder text from form fields and website-builder templates, not real contacts
PLACEHOLDER_EMAIL_LOCALS = {
    "you", "your", "yourname", "name", "john", "jane", "johndoe", "janedoe", "example", "test",
    "user", "username", "email", "emailaddress", "youremail", "filler", "xxx",
}
PLACEHOLDER_EMAIL_DOMAINS = {
    "company.com", "email.com", "domain.com", "yourdomain.com", "mysite.com", "website.com",
    "yourcompany.com", "youremail.com", "example.com", "example.org", "test.com", "address.com",
    "doe.com", "xxx.xxx", "godaddy.com", "domainmarket.com", "sample.com",
}

def is_valid_email(email):
    email = email.lower()
    local, _, domain = email.partition("@")
    if email.endswith(EXCLUDED_EMAIL_EXTS) or "example" in domain:
        return False
    if local in PLACEHOLDER_EMAIL_LOCALS or domain in PLACEHOLDER_EMAIL_DOMAINS:
        return False
    # Sentry/Wix tracking IDs look like emails: <32 hex chars>@sentry.wixpress.com
    if "sentry" in domain or "wixpress" in domain or re.fullmatch(r"[0-9a-f]{32}", local):
        return False
    return True

def normalize_phone(raw):
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10 or len(set(digits)) == 1 or digits[0] in "01" or digits[3] in "01":
        return None
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"

def extract_contacts_from_html(html_text):
    emails, phones = set(), set()

    for tel in TEL_LINK_REGEX.findall(html_text):
        phone = normalize_phone(tel)
        if phone:
            phones.add(phone)

    # Drop script/style blocks (tracking IDs, JSON) and decode entities like &#64; before matching
    visible_text = unescape(SCRIPT_STYLE_REGEX.sub(" ", html_text))
    emails.update(e.lower() for e in EMAIL_REGEX.findall(visible_text) if is_valid_email(e))
    for match in PHONE_REGEX.findall(visible_text):
        phone = normalize_phone(match)
        if phone:
            phones.add(phone)

    return emails, phones

def find_contact_page_urls(html_text, base_url):
    base_host = urlsplit(base_url).netloc.lower().removeprefix("www.")
    found = []
    for href in PAGE_LINK_REGEX.findall(html_text):
        if not any(hint in href.lower() for hint in CONTACT_PAGE_HINTS):
            continue
        absolute = urljoin(base_url, href.strip())
        parts = urlsplit(absolute)
        if parts.scheme in ("http", "https") and parts.netloc.lower().removeprefix("www.") == base_host:
            if absolute not in found:
                found.append(absolute)
    for path in FALLBACK_CONTACT_PATHS:
        absolute = urljoin(base_url, path)
        if absolute not in found:
            found.append(absolute)
    return found

def handle_async_exception(loop, context):
    """Suppresses background DNS resolution stack traces (socket.gaierror)."""
    if "exception" in context and isinstance(context["exception"], (OSError, asyncio.TimeoutError)):
        return
    loop.default_exception_handler(context)

def candidate_base_urls(host):
    bases = [f"https://{host}", f"http://{host}"]
    if not host.lower().startswith("www."):
        bases.append(f"https://www.{host}")
    return bases

async def fetch_html(session, url, semaphore):
    """Returns (html, final_url) on a 200 HTML response, otherwise (None, None)."""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        async with semaphore:
            async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT, ssl=False) as resp:
                if resp.status != 200:
                    return None, None
                content_type = resp.headers.get("Content-Type", "").lower()
                if content_type and "html" not in content_type and "text" not in content_type:
                    return None, None
                body = bytearray()
                async for chunk in resp.content.iter_chunked(65536):
                    body.extend(chunk)
                    if len(body) >= MAX_PAGE_BYTES:
                        break
                return body.decode("utf-8", errors="replace"), str(resp.url)
    except Exception:
        return None, None

async def scrape_website(session, website_url, semaphore):
    if not website_url or not isinstance(website_url, str):
        return "", "", True

    host = re.sub(r"^https?://", "", website_url.strip(), flags=re.IGNORECASE).strip("/")
    if not host:
        return "", "", True

    home_html, home_url = None, None
    for base in candidate_base_urls(host):
        home_html, home_url = await fetch_html(session, base, semaphore)
        if home_html is not None:
            break
    # No page could be loaded: report it so the site is retried, instead of treating it as "no contacts"
    if home_html is None:
        return "", "", False

    emails, phones = extract_contacts_from_html(home_html)
    if not (emails and phones):
        for page_url in find_contact_page_urls(home_html, home_url)[:MAX_EXTRA_PAGES]:
            page_html, _ = await fetch_html(session, page_url, semaphore)
            if page_html is None:
                continue
            page_emails, page_phones = extract_contacts_from_html(page_html)
            emails |= page_emails
            phones |= page_phones
            if emails and phones:
                break

    email_str = "; ".join(sorted(emails)[:3])
    phone_str = "; ".join(sorted(phones)[:2])
    return email_str, phone_str, True

def load_checkpoint(path):
    """Returns {company_id: (email, phone, fetched)} for sites saved in one checkpoint file."""
    saved = {}
    if not os.path.exists(path):
        return saved
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                record = json.loads(line)
                saved[record["id"]] = (record.get("email", ""), record.get("phone", ""), record.get("fetched", True))
            except (json.JSONDecodeError, KeyError):
                continue  # a line cut off by a crash or Ctrl+C
    return saved

def load_state(folder, prefix=FULL_RUN_PREFIX, include_legacy=True):
    """Merges every checkpoint file in order; later rounds override earlier ones."""
    state = load_checkpoint(os.path.join(folder, LEGACY_CHECKPOINT)) if include_legacy else {}
    for path in sorted(glob.glob(os.path.join(folder, f"{prefix}_r*_w*.jsonl"))):
        state.update(load_checkpoint(path))
    return state

def remove_checkpoints(folder, prefix):
    for path in glob.glob(os.path.join(folder, f"{prefix}_r*_w*.jsonl")):
        os.remove(path)

def open_checkpoint_for_append(path):
    # If the last line was cut off, start a new line so the next record isn't merged into it
    needs_newline = False
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "rb") as f:
            f.seek(-1, os.SEEK_END)
            needs_newline = f.read(1) != b"\n"
    f = open(path, "a", encoding="utf-8")
    if needs_newline:
        f.write("\n")
    return f

def wants(state, company_id, mode):
    """'initial': sites never fetched. 'retry': sites whose last attempt failed to load."""
    record = state.get(company_id)
    if mode == "initial":
        return record is None or not record[2]
    return record is not None and not record[2]

def filter_companies(matched_path):
    """Streams the big dataset once and writes only the matching California records to disk."""
    count = 0
    print("Parsing dataset for California B2B targets...")
    with open(input_file, "r", encoding="utf-8") as infile, open(matched_path, "w", encoding="utf-8") as out:
        for line in infile:
            if not line.strip():
                continue
            try:
                company = json.loads(line)
                country = company.get("country", "")
                region = company.get("region", "")

                if country and country.upper() == "UNITED STATES" and is_california(region):
                    matched_cat = classify_industry(company.get("industry", ""), company.get("name", ""))
                    if matched_cat:
                        company["matched_category"] = matched_cat
                        out.write(json.dumps(company) + "\n")
                        count += 1
            except json.JSONDecodeError:
                continue
    return count

def count_wanted(matched_path, state, mode):
    total = 0
    with open(matched_path, "r", encoding="utf-8") as f:
        for line in f:
            if wants(state, json.loads(line).get("id"), mode):
                total += 1
    return total

def bump(counters, key):
    """Increments a counter shared by all worker processes and returns the new value."""
    with counters[key].get_lock():
        counters[key].value += 1
        return counters[key].value

async def worker_main(cfg, worker_id, round_no, mode, counters):
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_async_exception)
    if not aiodns:
        loop.set_default_executor(ThreadPoolExecutor(max_workers=50))

    state = load_state(cfg["output_dir"], cfg["ckpt_prefix"], cfg["include_legacy"])
    semaphore = asyncio.Semaphore(cfg["connections"])
    # Bounds how many sites are in flight at once, so memory stays flat no matter how many sites there are
    window = asyncio.Semaphore(cfg["connections"] * 2)
    resolver = aiohttp.AsyncResolver(nameservers=DNS_SERVERS) if aiodns else None
    connector = aiohttp.TCPConnector(limit=cfg["connections"], ttl_dns_cache=300, resolver=resolver)
    checkpoint_path = os.path.join(cfg["output_dir"], f"{cfg['ckpt_prefix']}_r{round_no}_w{worker_id}.jsonl")
    checkpoint_file = open_checkpoint_for_append(checkpoint_path)
    tasks = set()

    async def process(comp):
        try:
            email, phone, fetched = await scrape_website(session, comp.get("website", ""), semaphore)
        except Exception:
            email, phone, fetched = "", "", False
        checkpoint_file.write(json.dumps({"id": comp.get("id"), "email": email, "phone": phone, "fetched": fetched}) + "\n")
        checkpoint_file.flush()
        done = bump(counters, "done")
        if email or phone:
            bump(counters, "found")
            print(f"[{done:,}/{counters['total'].value:,}] {comp.get('website')}: {email or '-'} | {phone or '-'}", flush=True)
        if done % 1000 == 0:
            found = counters["found"].value
            print(f"--- {done:,} done, {counters['total'].value - done:,} remaining, {found:,} with contacts so far", flush=True)

    def release(task):
        tasks.discard(task)
        window.release()

    async with aiohttp.ClientSession(connector=connector) as session:
        with open(cfg["matched_file"], "r", encoding="utf-8") as f:
            for index, line in enumerate(f):
                if index % cfg["workers"] != worker_id:
                    continue
                comp = json.loads(line)
                if not wants(state, comp.get("id"), mode):
                    continue
                await window.acquire()
                task = asyncio.create_task(process(comp))
                tasks.add(task)
                task.add_done_callback(release)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    checkpoint_file.close()

def run_worker(cfg, worker_id, round_no, mode, counters):
    asyncio.run(worker_main(cfg, worker_id, round_no, mode, counters))

def run_round(ctx, cfg, round_no, mode, state):
    total = count_wanted(cfg["matched_file"], state, mode)
    label = "Round 1: scraping sites not yet fetched" if mode == "initial" else "Round 2: retrying sites that could not be reached"
    print(f"{label}: {total:,} sites with {cfg['workers']} workers x {cfg['connections']} connections")
    if total == 0:
        return
    counters = {
        "done": ctx.Value("i", 0),
        "found": ctx.Value("i", sum(1 for email, phone, _ in state.values() if email or phone)),
        "total": ctx.Value("i", total),
    }
    procs = [ctx.Process(target=run_worker, args=(cfg, w, round_no, mode, counters)) for w in range(cfg["workers"])]
    for p in procs:
        p.start()
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        for p in procs:
            p.terminate()
        raise
    failed = [p.exitcode for p in procs if p.exitcode != 0]
    if failed:
        print(f"WARNING: {len(failed)} worker(s) exited with errors. Run the script again to resume.")

def run_scraping_rounds(cfg):
    """Round 1 scrapes everything not yet fetched, round 2 retries what failed. Returns False if stopped with Ctrl+C."""
    ctx = multiprocessing.get_context("spawn")
    try:
        for round_no, mode in ((1, "initial"), (2, "retry")):
            run_round(ctx, cfg, round_no, mode, load_state(cfg["output_dir"], cfg["ckpt_prefix"], cfg["include_legacy"]))
    except KeyboardInterrupt:
        print("\nStopped. Run the same command again to resume from the saved results.")
        return False
    return True

def build_row(company, matched_cat, scraped_email, scraped_phone):
    return [
        company.get("id"),
        company.get("name"),
        matched_cat.replace("_", " ").title(),
        company.get("industry"),
        company.get("country"),
        company.get("region"),
        company.get("locality"),
        company.get("website"),
        scraped_email,
        scraped_phone,
        company.get("founded"),
        company.get("size"),
        company.get("linkedin_url"),
    ]

def write_csv_set(rows, combined_filename):
    """Writes the combined CSV and one CSV per category from (category_key, row_data) pairs.
    Files are written under temporary names and renamed at the end, so a CSV being read as input can be replaced safely."""
    print("Writing scraped data to CSV files...")
    category_counts = Counter()
    file_handles = {}
    writers = {}
    final_names = {}
    total_records = 0

    combined_file = open(combined_filename + ".tmp", "w", newline="", encoding="utf-8")
    combined_writer = csv.writer(combined_file)
    combined_writer.writerow(fieldnames)

    for matched_cat, row_data in rows:
        total_records += 1
        category_counts[matched_cat] += 1
        combined_writer.writerow(row_data)

        if matched_cat not in writers:
            cat_filename = os.path.join(
                output_dir, f"philip_{today_str}_ca_{sanitize_filename(matched_cat)}_scraped.csv"
            )
            f = open(cat_filename + ".tmp", "w", newline="", encoding="utf-8")
            w = csv.writer(f)
            w.writerow(fieldnames)

            file_handles[matched_cat] = f
            writers[matched_cat] = w
            final_names[matched_cat] = cat_filename

        writers[matched_cat].writerow(row_data)

    # Append bottom summary row to each category CSV file
    for cat_key, w in writers.items():
        count_for_cat = category_counts[cat_key]
        clean_cat_title = cat_key.replace("_", " ").title()

        w.writerow([])
        w.writerow([
            "SUMMARY",
            f"Total {clean_cat_title} Records:",
            f"{count_for_cat:,}",
            "", "", "", "", "", "", "", "", "", ""
        ])

    # Append bottom summary row to combined CSV file
    combined_writer.writerow([])
    combined_writer.writerow([
        "SUMMARY",
        "Total All California B2B Records:",
        f"{total_records:,}",
        "", "", "", "", "", "", "", "", "", ""
    ])

    combined_file.close()
    for f in file_handles.values():
        f.close()
    os.replace(combined_filename + ".tmp", combined_filename)
    for cat_filename in final_names.values():
        os.replace(cat_filename + ".tmp", cat_filename)

    return category_counts

def unreached_site_entry(company_id, name, category_title, website):
    return {"id": company_id, "name": name, "category": category_title, "website": website}

def write_unreached_json(path, sites, source_csv, note):
    """Saves the sites that could not be reached, for a later run with --retry-unreached."""
    data = {
        "source_csv": os.path.basename(source_csv),
        "created": today_str,
        "note": note,
        "count": len(sites),
        "sites": sites,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1)

def print_summary(title, lines, category_counts):
    print("\n" + "="*40)
    print(f" {title}")
    for line in lines:
        print(f" {line}")
    print("="*40)
    print(" BREAKDOWN BY CATEGORY:")
    for cat, count in category_counts.most_common():
        clean_cat = cat.replace("_", " ").title()
        print(f" - {clean_cat:<30}: {count:,} records")
    print("="*40 + "\n")

def main():
    combined_filename = os.path.join(output_dir, f"philip_{today_str}_california_all_b2b_scraped.csv")
    unreached_filename = os.path.join(output_dir, f"philip_{today_str}_ca_unreached_sites.json")
    matched_path = os.path.join(output_dir, "matched_companies.jsonl")

    total_records = filter_companies(matched_path)
    print(f"Found {total_records:,} matched California B2B records.")
    print(f"Settings: {WORKERS} worker processes x {CONNECTIONS_PER_WORKER} connections; "
          f"DNS resolver: " + (f"aiodns via {', '.join(DNS_SERVERS)}" if aiodns else "threaded (run 'pip install aiodns' to speed this up)"))

    cfg = {
        "output_dir": output_dir,
        "matched_file": matched_path,
        "workers": WORKERS,
        "connections": CONNECTIONS_PER_WORKER,
        "ckpt_prefix": FULL_RUN_PREFIX,
        "include_legacy": True,
    }
    if not run_scraping_rounds(cfg):
        return

    state = load_state(output_dir)
    stats = Counter()
    unreached = []

    def full_run_rows():
        with open(matched_path, "r", encoding="utf-8") as matched:
            for line in matched:
                company = json.loads(line)
                matched_cat = company["matched_category"]
                scraped_email, scraped_phone, fetched = state.get(company.get("id"), ("", "", False))
                if not fetched:
                    unreached.append(unreached_site_entry(
                        company.get("id"), company.get("name"), matched_cat.replace("_", " ").title(), company.get("website")
                    ))
                if scraped_email or scraped_phone:
                    stats["with_contacts"] += 1
                yield matched_cat, build_row(company, matched_cat, scraped_email, scraped_phone)

    category_counts = write_csv_set(full_run_rows(), combined_filename)
    write_unreached_json(
        unreached_filename, unreached, combined_filename,
        "Sites whose pages could not be loaded (timeouts, DNS failures, blocked requests)."
    )

    # All CSVs are written, so the intermediate files are no longer needed
    for path in (matched_path, os.path.join(output_dir, LEGACY_CHECKPOINT)):
        if os.path.exists(path):
            os.remove(path)
    remove_checkpoints(output_dir, FULL_RUN_PREFIX)

    print_summary(f"SCRAPING COMPLETE: {total_records:,} RECORDS PROCESSED", [
        f"SITES WITH EMAIL OR PHONE: {stats['with_contacts']:,}",
        f"SITES NOT REACHED (even after retry): {len(unreached):,}",
        f"UNREACHED SITES SAVED TO: {unreached_filename}",
    ], category_counts)

def main_retry(json_path):
    """Scrapes only the sites in an unreached-sites JSON file and merges what it finds into that file's CSVs."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    base_csv = os.path.join(output_dir, data["source_csv"])
    if not os.path.exists(base_csv):
        print(f"Cannot find {base_csv}, which this file's results belong to.")
        return
    sites = data["sites"]
    combined_filename = os.path.join(output_dir, f"philip_{today_str}_california_all_b2b_scraped.csv")
    unreached_filename = os.path.join(output_dir, f"philip_{today_str}_ca_unreached_sites.json")
    sites_path = os.path.join(output_dir, "retry_sites.jsonl")

    print(f"Retrying {len(sites):,} unreached sites from {os.path.basename(json_path)}; "
          f"results will be merged into {data['source_csv']}")
    with open(sites_path, "w", encoding="utf-8") as out:
        for site in sites:
            out.write(json.dumps(site) + "\n")

    cfg = {
        "output_dir": output_dir,
        "matched_file": sites_path,
        "workers": WORKERS,
        "connections": CONNECTIONS_PER_WORKER,
        "ckpt_prefix": RETRY_RUN_PREFIX,
        "include_legacy": False,
    }
    if not run_scraping_rounds(cfg):
        return

    state = load_state(output_dir, RETRY_RUN_PREFIX, include_legacy=False)
    stats = Counter()

    def merged_rows():
        with open(base_csv, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if len(row) < len(fieldnames) or row[0] in ("", "SUMMARY"):
                    continue
                result = state.get(row[0])
                # Only fill in what the retry found; never blank out earlier results
                if result and (result[0] or result[1]):
                    if not (row[8] or row[9]):
                        stats["newly_found"] += 1
                    row[8] = result[0] or row[8]
                    row[9] = result[1] or row[9]
                if row[8] or row[9]:
                    stats["with_contacts"] += 1
                yield row[2].lower().replace(" ", "_"), row

    category_counts = write_csv_set(merged_rows(), combined_filename)

    still_unreached = [site for site in sites if not state.get(site["id"], ("", "", False))[2]]
    write_unreached_json(
        unreached_filename, still_unreached, combined_filename,
        "Sites still not reachable after a --retry-unreached run."
    )
    if os.path.exists(sites_path):
        os.remove(sites_path)
    remove_checkpoints(output_dir, RETRY_RUN_PREFIX)

    print_summary(f"RETRY COMPLETE: {len(sites):,} SITES RETRIED", [
        f"NEW SITES WITH EMAIL OR PHONE: {stats['newly_found']:,}",
        f"TOTAL SITES WITH EMAIL OR PHONE: {stats['with_contacts']:,}",
        f"STILL NOT REACHED: {len(still_unreached):,}",
        f"UPDATED CSVs: {os.path.basename(combined_filename)} (+ one per category)",
        f"REMAINING UNREACHED SITES SAVED TO: {unreached_filename}",
    ], category_counts)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape contact info for California B2B companies.")
    parser.add_argument(
        "--retry-unreached", metavar="JSON_FILE",
        help="only scrape the sites listed in an unreached-sites JSON file and merge the results into its CSVs",
    )
    args = parser.parse_args()
    if args.retry_unreached:
        main_retry(args.retry_unreached)
    else:
        main()
