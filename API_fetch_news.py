import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timezone, timedelta
from email.message import EmailMessage
import smtplib
import os
import sys
import time
import json
import csv
import signal
import functools


# ==============================================================
# .env FILE LOADER
# ==============================================================

def load_env_file():
    """Load environment variables from .env file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    possible_paths = [
        os.path.join(script_dir, ".env"),
        os.path.join(os.getcwd(), ".env"),
    ]
    env_path = None
    for p in possible_paths:
        if os.path.exists(p):
            env_path = p
            break
    if env_path is None:
        print("   No .env file found. Using system environment variables.", flush=True)
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path, override=True)
        print(f"   Loaded .env via python-dotenv: {env_path}", flush=True)
        return
    except ImportError:
        pass
    loaded_keys = []
    with open(env_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("set "):
                line = line[4:].strip()
            if line.lower().startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            if key:
                os.environ[key] = value
                loaded_keys.append(key)
    if loaded_keys:
        print(f"   Loaded .env (manual): {env_path}", flush=True)
        print(f"      Variables: {', '.join(loaded_keys)}", flush=True)


print("Loading environment...", flush=True)
load_env_file()


# ==============================================================
# CONFIGURATION
# ==============================================================
IST = timezone(timedelta(hours=5, minutes=30))
TODAY = datetime.now(IST).strftime("%Y-%m-%d")
TODAY_DISPLAY = datetime.now(IST).strftime("%d-%m-%Y")
OUTPUT_FILE = f"CompanyNews_{TODAY}.xlsx"
CSV_FILE = f"CompanyNews_{TODAY}.csv"

IS_GITHUB = os.environ.get("GITHUB_ACTIONS") == "true"
if IS_GITHUB:
    COMPANY_FILE = "data/CompanyList.xlsx"
    OUTPUT_DIR = "output"
else:
    BASE_DIR = os.environ.get("NEWS_BASE_DIR", r"C:\NewsAutomation")
    COMPANY_FILE = os.path.join(BASE_DIR, "CompanyList.xlsx")
    OUTPUT_DIR = os.path.join(BASE_DIR, "output")

API_BASE_URL = "https://api.freenewsapi.io/v1/news"
API_KEY = os.environ.get("FREENEWS_API_KEY", "")

if API_KEY:
    masked = API_KEY[:4] + "****" + API_KEY[-4:] if len(API_KEY) > 12 else "***"
    print(f"   API Key: {masked}", flush=True)
else:
    print("   WARNING: API Key NOT FOUND!", flush=True)

RATE_LIMIT_DELAY = 0.55
QUOTA_SAFETY_BUFFER = 200
MAX_RETRIES = 5
REQUEST_TIMEOUT = 20
CONNECTION_LIMIT = 10
BATCH_SIZE = 50
LANGUAGE = "en"
COUNTRY = "IN"
HOURS_LOOKBACK = 24

# Email - ALL values from environment/secrets, NO hardcoded defaults
EMAIL_FROM = os.environ.get("EMAIL_FROM", "")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "")
EMAIL_TO = os.environ.get("EMAIL_TO", "")
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))

# Connectivity test keyword - generic, no company-specific info
TEST_KEYWORD = os.environ.get("TEST_KEYWORD", "India business")

HTTPS_PROXY = os.environ.get("HTTPS_PROXY", "")


# -- GLOBAL STATE --
rate_limit_lock = asyncio.Lock()
last_request_time = 0.0
quota_remaining = 5000
quota_exhausted = False
graceful_stop = False


def log(msg):
    print(msg, flush=True)


def format_time(seconds):
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}m {secs}s"


def get_published_after():
    dt = datetime.now(timezone.utc) - timedelta(hours=HOURS_LOOKBACK)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def utc_to_ist(utc_str):
    try:
        if utc_str.endswith("Z"):
            utc_str = utc_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(utc_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        ist_dt = dt.astimezone(IST)
        return ist_dt.strftime("%Y-%m-%d %H:%M:%S IST")
    except Exception:
        return utc_str


def clean_keyword(keyword):
    if not keyword or not isinstance(keyword, str):
        return None
    keyword = keyword.strip()
    if keyword.lower() == "nan" or keyword == "":
        return None
    if len(keyword) < 2:
        return None
    return keyword


async def enforce_rate_limit():
    global last_request_time
    async with rate_limit_lock:
        now = asyncio.get_event_loop().time()
        wait_time = max(0, RATE_LIMIT_DELAY - (now - last_request_time))
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        last_request_time = asyncio.get_event_loop().time()


def update_quota_from_headers(headers):
    global quota_remaining
    val = headers.get("X-RateLimit-Remaining-Day")
    if val is not None:
        try:
            quota_remaining = int(val)
        except (ValueError, TypeError):
            pass


def check_quota():
    global quota_exhausted
    if quota_remaining <= QUOTA_SAFETY_BUFFER:
        if not quota_exhausted:
            log(f"   WARNING: Quota nearly exhausted! Remaining: {quota_remaining}")
            quota_exhausted = True
        return False
    return True


def quota_exhausted_flag():
    global quota_exhausted
    quota_exhausted = True


# ==============================================================
# INCREMENTAL CSV FUNCTIONS
# ==============================================================

CSV_COLUMNS = ["Company", "Keyword", "Article_UUID", "Title",
               "Published_UTC", "Published_IST", "Publisher"]


def init_csv(csv_path):
    if not os.path.exists(csv_path):
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_COLUMNS)
        log(f"   Created: {csv_path}")
    else:
        rows = count_csv_rows(csv_path)
        log(f"   CSV exists with {rows} articles (will append)")


def append_to_csv(csv_path, articles):
    if not articles:
        return
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for a in articles:
            writer.writerow([
                a.get("Company", ""), a.get("Keyword", ""),
                a.get("Article_UUID", ""), a.get("Title", ""),
                a.get("Published_UTC", ""), a.get("Published_IST", ""),
                a.get("Publisher", ""),
            ])


def count_csv_rows(csv_path):
    try:
        if not os.path.exists(csv_path):
            return 0
        with open(csv_path, "r", encoding="utf-8") as f:
            return max(0, sum(1 for _ in f) - 1)
    except Exception:
        return 0


def csv_to_excel(csv_path, excel_path):
    try:
        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            log("   WARNING: CSV not found or empty.")
            return 0
        df = pd.read_csv(csv_path, encoding="utf-8")
        if len(df) == 0:
            log("   No data rows.")
            return 0
        before = len(df)
        df.drop_duplicates(subset=["Article_UUID"], keep="first", inplace=True)
        dupes = before - len(df)
        if dupes > 0:
            log(f"   Removed {dupes} duplicates")
        df.sort_values(by=["Company", "Published_UTC"],
                       ascending=[True, False], inplace=True)
        df.to_excel(excel_path, index=False, engine="openpyxl")
        log(f"   Excel: {excel_path} ({len(df)} articles)")
        return len(df)
    except Exception as e:
        log(f"   ERROR CSV to Excel: {e}")
        return 0


# ==============================================================
# CORE API FUNCTION
# ==============================================================

async def fetch_news_for_company(session, company_name, keyword):
    if not check_quota():
        return []
    params = {
        "in_title": keyword,
        "language": LANGUAGE,
        "country": COUNTRY,
        "published_after": get_published_after(),
        "order_by": "recent",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        await enforce_rate_limit()
        try:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            async with session.get(API_BASE_URL, params=params,
                                   timeout=timeout) as resp:
                update_quota_from_headers(resp.headers)
                if resp.status == 200:
                    body = await resp.json()
                    articles = []
                    for item in body.get("data", []):
                        pub_utc = item.get("published_at", "")
                        articles.append({
                            "Company": company_name,
                            "Keyword": keyword,
                            "Article_UUID": item.get("uuid", ""),
                            "Title": item.get("title", ""),
                            "Published_UTC": pub_utc,
                            "Published_IST": utc_to_ist(pub_utc),
                            "Publisher": item.get("publisher", ""),
                        })
                    return articles
                elif resp.status == 429:
                    try:
                        err = await resp.json()
                        error_msg = err.get("error", "")
                        retry_ms = err.get("retry_after_ms", 1000)
                    except Exception:
                        error_msg = ""
                        retry_ms = 1000
                    if "quota exceeded" in error_msg.lower():
                        log(f"   QUOTA EXCEEDED: {error_msg}")
                        quota_exhausted_flag()
                        return []
                    wait = retry_ms / 1000.0
                    log(f"   429 for '{keyword}' ({attempt}/{MAX_RETRIES}), retry {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                elif resp.status == 400:
                    try:
                        err = await resp.json()
                        error_msg = err.get("error", "Unknown")
                    except Exception:
                        error_msg = "Unknown"
                    log(f"   400 for '{keyword}': {error_msg}")
                    return []
                elif resp.status in (401, 403):
                    try:
                        err = await resp.json()
                        error_msg = err.get("error", "Auth error")
                    except Exception:
                        error_msg = "Auth error"
                    log(f"   AUTH ERROR {resp.status}: {error_msg}")
                    quota_exhausted_flag()
                    return []
                else:
                    log(f"   HTTP {resp.status} for '{keyword}' ({attempt}/{MAX_RETRIES})")
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(2 ** attempt)
                    continue
        except asyncio.TimeoutError:
            log(f"   Timeout '{keyword}' ({attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2 ** attempt)
            continue
        except aiohttp.ClientError as exc:
            log(f"   Connection error '{keyword}' ({attempt}/{MAX_RETRIES}): {exc}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2 ** attempt)
            continue
        except Exception as exc:
            log(f"   Error '{keyword}' ({attempt}/{MAX_RETRIES}): {exc}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2 ** attempt)
            continue
    log(f"   FAILED all {MAX_RETRIES} attempts for '{keyword}'")
    return []


# ==============================================================
# CONNECTIVITY TEST
# ==============================================================

async def test_connectivity(session):
    log(f"\nCONNECTIVITY TEST - searching '{TEST_KEYWORD}' in title...")
    if not API_KEY:
        log("   ERROR: FREENEWS_API_KEY is not set!")
        return False
    params = {
        "in_title": TEST_KEYWORD,
        "language": LANGUAGE,
        "country": COUNTRY,
        "published_after": get_published_after(),
        "order_by": "recent",
    }
    try:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get(API_BASE_URL, params=params,
                               timeout=timeout) as resp:
            update_quota_from_headers(resp.headers)
            if resp.status == 200:
                body = await resp.json()
                count = body.get("meta", {}).get("returned", 0)
                log(f"   OK! {count} article(s). Quota: {quota_remaining}")
                return True
            elif resp.status == 401:
                log("   ERROR: Invalid API key!")
                return False
            elif resp.status == 403:
                log("   ERROR: API key disabled/expired!")
                return False
            else:
                log(f"   Unexpected status {resp.status}")
                return False
    except Exception as exc:
        log(f"   Connection failed: {exc}")
        return False


# ==============================================================
# COLUMN DETECTION
# ==============================================================

def detect_columns(df):
    cols_lower = {c.strip().lower(): c for c in df.columns}
    for candidate in ["keyword", "keyword1", "keywords", "search_keyword",
                       "searchkeyword"]:
        if candidate in cols_lower:
            keyword_col = cols_lower[candidate]
            break
    else:
        keyword_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
    for candidate in ["companyname", "company_name", "company", "name",
                       "borrower", "borrowername"]:
        if candidate in cols_lower:
            name_col = cols_lower[candidate]
            break
    else:
        name_col = df.columns[0]
    return name_col, keyword_col


# ==============================================================
# SIGNAL HANDLER
# ==============================================================

def handle_interrupt(csv_path, excel_path, signum, frame):
    global graceful_stop
    if graceful_stop:
        print("\nForce quit! Converting CSV to Excel...", flush=True)
        csv_to_excel(csv_path, excel_path)
        sys.exit(1)
    print("\n\nCtrl+C detected! Finishing current batch...", flush=True)
    graceful_stop = True


# ==============================================================
# EMAIL
# ==============================================================

def send_email(file_path, article_count):
    """Send email with Excel attached. All config from environment."""
    if not EMAIL_FROM or not EMAIL_PASSWORD or not EMAIL_TO:
        log("Email not configured (set EMAIL_FROM, EMAIL_PASSWORD, EMAIL_TO).")
        return False
    try:
        msg = EmailMessage()
        msg["Subject"] = f"Daily Company News - {TODAY_DISPLAY} ({article_count} articles)"
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO.replace(";", ", ")
        msg.set_content(
            f"Hi Team,\n\n"
            f"Please find attached the daily company news report for {TODAY_DISPLAY}.\n\n"
            f"  - Articles found : {article_count}\n"
            f"  - Source          : FreeNews API\n"
            f"  - Language        : English\n"
            f"  - Country         : India\n"
            f"  - Period          : Last {HOURS_LOOKBACK} hours\n"
            f"  - Match criteria  : Company keyword in article title\n\n"
            f"Regards,\nNews Automation"
        )
        with open(file_path, "rb") as f:
            file_data = f.read()
            file_name = os.path.basename(file_path)
            msg.add_attachment(
                file_data,
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=file_name,
            )
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=60) as server:
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.send_message(msg)
        log(f"Email sent successfully to {len(EMAIL_TO.split(';'))} recipient(s).")
        return True
    except Exception as exc:
        log(f"Email failed: {exc}")
        return False


# ==============================================================
# MAIN
# ==============================================================

async def main():
    global graceful_stop
    start_time = time.time()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, CSV_FILE)
    excel_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    handler = functools.partial(handle_interrupt, csv_path, excel_path)
    signal.signal(signal.SIGINT, handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handler)

    log("=" * 64)
    log("COMPANY NEWS FETCHER - FreeNews API")
    log(f"Date       : {TODAY_DISPLAY}")
    log(f"Search     : Title only")
    log(f"Filters    : language={LANGUAGE} | country={COUNTRY} | last {HOURS_LOOKBACK}h")
    log(f"Batch save : Every {BATCH_SIZE} companies")
    log(f"Platform   : {'GitHub Actions' if IS_GITHUB else 'Local PC'}")
    log("=" * 64)

    if not API_KEY:
        log("\nERROR: FREENEWS_API_KEY not set!")
        log("Set it in .env file or as environment variable.")
        sys.exit(1)

    try:
        df_companies = pd.read_excel(COMPANY_FILE)
        log(f"\nLoaded {len(df_companies)} rows from {COMPANY_FILE}")
    except FileNotFoundError:
        log(f"\nERROR: File not found: {COMPANY_FILE}")
        sys.exit(1)
    except Exception as exc:
        log(f"\nERROR: {exc}")
        sys.exit(1)

    name_col, keyword_col = detect_columns(df_companies)
    log(f"   Columns: Name='{name_col}' | Keyword='{keyword_col}'")

    companies = []
    skipped = 0
    for _, row in df_companies.iterrows():
        raw_name = str(row[name_col]).strip()
        raw_kw = str(row[keyword_col]).strip()
        kw = clean_keyword(raw_kw)
        if kw is None:
            skipped += 1
            continue
        companies.append((raw_name, kw))

    log(f"   Valid: {len(companies)} | Skipped: {skipped}")

    if not companies:
        log("ERROR: No valid companies!")
        sys.exit(1)

    est_time = len(companies) * RATE_LIMIT_DELAY
    log(f"\nEstimated time: ~{format_time(est_time)}")
    log(f"API calls needed: {len(companies)}\n")

    init_csv(csv_path)

    headers_dict = {"x-api-key": API_KEY, "Accept": "application/json"}
    connector = aiohttp.TCPConnector(limit=CONNECTION_LIMIT, ttl_dns_cache=300)

    async with aiohttp.ClientSession(headers=headers_dict,
                                      connector=connector) as session:

        if not await test_connectivity(session):
            log("\nConnectivity test failed. Exiting.")
            sys.exit(1)

        log(f"\n{'=' * 64}")
        log(f"Searching titles ({len(companies)} companies)")
        log(f"{'=' * 64}")

        search_start = time.time()
        total_articles = count_csv_rows(csv_path)
        total_batches = (len(companies) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_idx in range(0, len(companies), BATCH_SIZE):
            if graceful_stop:
                log("\nGraceful stop requested...")
                break
            if not check_quota():
                log("   Quota low - stopping.")
                break

            batch = companies[batch_idx: batch_idx + BATCH_SIZE]
            batch_num = (batch_idx // BATCH_SIZE) + 1
            batch_articles = []

            for company_name, keyword in batch:
                if graceful_stop or not check_quota():
                    break
                articles = await fetch_news_for_company(
                    session, company_name, keyword
                )
                if articles:
                    batch_articles.extend(articles)

            # SAVE TO CSV AFTER EVERY BATCH
            append_to_csv(csv_path, batch_articles)
            total_articles += len(batch_articles)

            done = min(batch_idx + BATCH_SIZE, len(companies))
            elapsed = time.time() - search_start
            rate = done / elapsed if elapsed > 0 else 0
            eta = (len(companies) - done) / rate if rate > 0 else 0
            log(
                f"   Batch {batch_num}/{total_batches} | "
                f"{done}/{len(companies)} | "
                f"+{len(batch_articles)} (total: {total_articles}) | "
                f"Quota: {quota_remaining} | "
                f"ETA: {format_time(eta)}"
            )

        search_time = time.time() - search_start
        log(f"\nSearch complete: {total_articles} articles in {format_time(search_time)}")

    # Convert CSV to Excel
    log(f"\n{'=' * 64}")
    log("Converting CSV to Excel...")
    final_count = csv_to_excel(csv_path, excel_path)

    # Send email
    if final_count > 0:
        send_email(excel_path, final_count)
    else:
        log("No articles found - skipping email.")

    # Summary
    total_time = time.time() - start_time
    log(f"\n{'=' * 64}")
    log("SUMMARY")
    log(f"{'=' * 64}")
    log(f"   Articles   : {final_count}")
    log(f"   Quota left : {quota_remaining}")
    log(f"   Time       : {format_time(total_time)}")
    log(f"   CSV        : {csv_path}")
    log(f"   Excel      : {excel_path}")
    if graceful_stop:
        log("   NOTE: Partial run (interrupted)")
    log(f"{'=' * 64}")
    log("Done!")


if __name__ == "__main__":
    asyncio.run(main())
