#!/usr/bin/env python3
"""
CTRI Cancer Clinical Trials Scraper v2 - Full PDF Extraction
=============================================================
Section-aware PDF parsing, fixed schema, unique Scraped_ID per row,
full contact attribution (PI / Scientific / Public separately).
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    WebDriverException, TimeoutException,
    NoSuchElementException, StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import pdfplumber
import requests
import time
import logging
import re
import io
import tempfile
import shutil
import sys
import os
import html as html_module
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue


# Set output encoding to utf-8 to handle special characters in Windows terminal
if sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        # Fallback for older python versions if needed
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())


# ==============================================================================
# LOGGING
# ==============================================================================
LOG_FILE = "ctri_final.log"
CSV_PROGRESS = "ctri_cancer_trials_PROGRESS.csv"
CSV_FINAL = "ctri_cancer_trials_FINAL.csv"
NUM_TABS = 3  # Number of concurrent tabs/PDF downloads

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8", mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("CTRIScraper")


def banner(text, char="=", width=72):
    line = char * width
    print(f"\n{line}\n  {text}\n{line}")


# ==============================================================================
# FIXED OUTPUT SCHEMA
# ==============================================================================
FIXED_COLUMNS = [
    "Scraped_ID",
    "CTRI_Number", "Registration_Date", "Last_Modified_Date",
    "Secondary_IDs", "Post_Graduate_Thesis",
    "Type_of_Trial", "Type_of_Study", "Study_Design", "Phase_of_Trial",
    "Blinding_Masking", "Randomization_Method", "Method_of_Concealment",
    "Public_Title", "Scientific_Title", "Trial_Acronym", "Brief_Summary",
    "Health_Condition",
    "Intervention_Name", "Intervention_Details",
    "Comparator_Name", "Comparator_Details",
    "Inclusion_Criteria", "Exclusion_Criteria",
    "Age_From", "Age_To", "Gender",
    "Mutation_Inclusion_Criteria", "Line_of_Therapy", "Stage_Requirements",
    "ECOG_Performance_Status", "Prior_Treatment_Requirements",
    "PI_Name", "PI_Designation", "PI_Affiliation",
    "PI_Address", "PI_Phone", "PI_Fax", "PI_Email",
    "Scientific_Contact_Name", "Scientific_Contact_Designation",
    "Scientific_Contact_Affiliation", "Scientific_Contact_Address",
    "Scientific_Contact_Phone", "Scientific_Contact_Fax", "Scientific_Contact_Email",
    "Public_Contact_Name", "Public_Contact_Designation",
    "Public_Contact_Affiliation", "Public_Contact_Address",
    "Public_Contact_Phone", "Public_Contact_Fax", "Public_Contact_Email",
    "Primary_Sponsor_Name", "Primary_Sponsor_Address", "Primary_Sponsor_Type",
    "Secondary_Sponsors", "Source_of_Funding",
    "Sites_of_Study",
    "Recruitment_Status_India", "Recruitment_Status_Global",
    "Countries_of_Recruitment",
    "Sample_Size_India", "Sample_Size_Total",
    "Final_Enrollment_India", "Final_Enrollment_Total",
    "Date_First_Enrollment_India", "Date_First_Enrollment_Global",
    "Estimated_Duration",
    "Date_Study_Completion_India", "Date_Study_Completion_Global",
    "Primary_Outcome", "Primary_Outcome_Timepoints",
    "Secondary_Outcome", "Secondary_Outcome_Timepoints",
    "Ethics_Committee", "DCGI_Status",
    "Publication_Details", "Search_Category", "Source_URL", "Scraped_At",
    "Uncategorized_Data",
]

# ==============================================================================
# SCRAPER
# ==============================================================================
class CTRIScraper:
    CTRI_URL = "https://ctri.nic.in/Clinicaltrials/pubview.php"
    PDF_URL = "https://ctri.nic.in/Clinicaltrials/pdf_generate.php"

    SEARCH_CATEGORIES = [
        ("Public title of study", "11"),
        ("Scientific title of study", "1"),
        ("Health Condition/ Problem Studied", "2"),
        # ("Name of Principle Investigator", "3"), not going to this category because has 0 trials
        ("Intervention and comparator agent", "4"),
        ("Primary outcome/s", "6"),
        ("Secondary outcome/s", "7"),
        ("Site/s of study", "8"),
        ("Ethics Committee", "9"),
        ("Primary Sponsor", "10"),
    ]

    def __init__(self):
        self.all_trials = []
        self.failed_urls = []
        self.stats = {"scraped": 0, "failed": 0}
        self.scraped_counter = 0

        # Pool of drivers and their locks
        self.driver_pool = Queue()
        self.drivers_list = []
        self._thread_local = threading.local()

        # Timings
        self.page_wait = 10
        self.between_trials = 0.5
        self.save_every = 10

        # Section tracking for contact attribution during PDF parse
        self._current_section = "header"
        
        # Shutdown flag
        self.interrupted = False

        # Load existing progress if any
        self._load_existing_progress()

    @property
    def driver(self):
        """Get the WebDriver instance assigned to the current thread."""
        if not hasattr(self._thread_local, "driver"):
            # If a thread doesn't have a driver, something is wrong with the pool management
            # In a pool model, the thread should get it from self.driver_pool
            return None
        return self._thread_local.driver

    @property
    def http_session(self):
        """Get the requests session assigned to the current thread."""
        if not hasattr(self._thread_local, "session"):
            self._thread_local.session = requests.Session()
        return self._thread_local.session

    @property
    def wait(self):
        """Get the WebDriverWait instance for the current thread's driver."""
        if not hasattr(self._thread_local, "wait") or self._thread_local.wait is None:
            if self.driver:
                self._thread_local.wait = WebDriverWait(self.driver, self.page_wait)
            else:
                return None
        return self._thread_local.wait

    # ------------------------------------------------------------------
    # PROGRESS
    # ------------------------------------------------------------------
    def _enforce_schema(self, df):
        for col in FIXED_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        # Also initialize derived tag columns to empty strings if not present
        derived_cols = [
            "Mutation_Inclusion_Criteria", "Line_of_Therapy", "Stage_Requirements",
            "ECOG_Performance_Status", "Prior_Treatment_Requirements"
        ]
        for dc in derived_cols:
            if dc not in df.columns:
                df[dc] = ""
        return df[FIXED_COLUMNS]

    def _sanitize_value(self, val):
        """Remove surrogate characters that cause utf-8 encoding errors."""
        if not isinstance(val, str):
            return val
        # Use encode/decode with 'ignore' to strip surrogate pairs that break CSV writing
        try:
            return val.encode("utf-8", "ignore").decode("utf-8")
        except Exception:
            return str(val)

    def _sanitize_data(self, data_list):
        """Sanitize all fields in the trial list."""
        sanitized = []
        for entry in data_list:
            sanitized.append({k: self._sanitize_value(v) for k, v in entry.items()})
        return sanitized

    def _safe_csv_write(self, df, filepath):
        """Write CSV using temp file + rename to avoid Permission denied errors."""
        try:
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", dir=os.path.dirname(os.path.abspath(filepath)))
            os.close(tmp_fd)
            df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
            # Try to replace the target file
            try:
                shutil.move(tmp_path, filepath)
            except PermissionError:
                # File is open in another program (e.g. Excel)
                logger.warning(f"File locked: {filepath}. Skipping this save to avoid redundant files.")
                # We don't create backups anymore as requested.
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            return True
        except Exception as e:
            logger.error(f"CSV write error: {e}")
            # Clean up temp file if it exists
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            return False

    def _load_existing_progress(self):
        """Load existing progress from CSV to allow resuming without data loss."""
        if os.path.exists(CSV_PROGRESS):
            try:
                df = pd.read_csv(CSV_PROGRESS)
                if not df.empty:
                    # Convert DataFrame to list of dicts
                    # Replace NaN with empty strings
                    self.all_trials = df.fillna("").to_dict("records")
                    self.scraped_counter = len(self.all_trials)
                    self.stats["scraped"] = len(self.all_trials)
                    logger.info(f"📂 Loaded {len(self.all_trials)} existing trials from {CSV_PROGRESS}")
            except Exception as e:
                logger.error(f"Error loading existing progress: {e}")

    def _save_progress(self):
        if not self.all_trials:
            return
        try:
            clean_trials = self._sanitize_data(self.all_trials)
            df = self._enforce_schema(pd.DataFrame(clean_trials))
            self._safe_csv_write(df, CSV_PROGRESS)
            logger.info(f"💾 Progress saved: {len(self.all_trials)} trials")
        except Exception as e:
            logger.error(f"Save error: {e}")

    def _save_final(self):
        if not self.all_trials:
            logger.warning("No data to save!")
            return
        clean_trials = self._sanitize_data(self.all_trials)
        df = self._enforce_schema(pd.DataFrame(clean_trials))
        self._safe_csv_write(df, CSV_FINAL)
        logger.info(f"✅ FINAL: {len(df)} trials → {CSV_FINAL}")

    # ------------------------------------------------------------------
    # DRIVER  (uses Brave browser to avoid popups/ads)
    # ------------------------------------------------------------------
    BRAVE_PATH = r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe"

    def _setup_driver(self):
        """Create a new Brave WebDriver instance."""
        if self.interrupted:
            return None
        logger.info("🌐 Creating new Brave WebDriver instance...")
        opts = Options()
        opts.binary_location = self.BRAVE_PATH
        opts.add_argument("--start-maximized")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-popup-blocking")
        opts.add_argument("--disable-notifications")
        # opts.add_argument("--headless=new")  # Disabled as user wants to see browsers
        opts.page_load_strategy = "eager"

        # Block images and plugins - keeping these enabled for visibility but can be re-enabled for speed
        # prefs = {
        #     "profile.managed_default_content_settings.images": 2,
        #     "profile.default_content_setting_values.notifications": 2,
        #     "profile.managed_default_content_settings.plugins": 2,
        #     "profile.managed_default_content_settings.popups": 2,
        #     "profile.managed_default_content_settings.geolocation": 2,
        # }
        # opts.add_experimental_option("prefs", prefs)

        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=opts,
            )
            logger.info("✓ WebDriver instance ready")
            return driver
        except Exception as e:
            logger.error(f"❌ Driver setup failed: {e}")
            return None

    def _init_pool(self):
        """Initialize the pool of WebDrivers."""
        banner(f"INITIALIZING {NUM_TABS} BROWSERS", "-")
        for i in range(NUM_TABS):
            drv = self._setup_driver()
            if drv:
                self.drivers_list.append(drv)
                self.driver_pool.put(drv)
        return len(self.drivers_list) > 0

    def _close_pool(self):
        """Close all WebDrivers in the pool."""
        logger.info(f"🛑 Closing {len(self.drivers_list)} WebDrivers...")
        for drv in self.drivers_list:
            try:
                drv.quit()
            except Exception:
                pass
        self.drivers_list = []
        # Clear the queue
        while not self.driver_pool.empty():
            self.driver_pool.get()

    def _driver_alive(self):
        try:
            _ = self.driver.current_url
            return True
        except Exception:
            return False

    def _recover(self):
        # Disabled as per user request to stop recovery attempts
        logger.warning("🔄 Recovery disabled.")
        return False

    def _sync_cookies_to_requests(self):
        """Copy Selenium cookies → requests session so we can download PDFs."""
        self.http_session.cookies.clear()
        for cookie in self.driver.get_cookies():
            self.http_session.cookies.set(
                cookie["name"], cookie["value"],
                domain=cookie.get("domain", ""),
                path=cookie.get("path", "/"),
            )

    # ------------------------------------------------------------------
    # POPUP / CELEBRATION PAGE HANDLING
    # ------------------------------------------------------------------
    JUNK_URLS = ["celebration.html", "celebration.php", "popup", "advertisement"]

    def _dismiss_popups(self):
        """
        Close any celebration/promotional tabs that the CTRI site opens.
        Returns control to the main working tab.
        """
        try:
            handles = self.driver.window_handles
            if len(handles) <= 1:
                return

            current = self.driver.current_window_handle
            for h in handles:
                if h == current:
                    continue
                try:
                    self.driver.switch_to.window(h)
                    url = self.driver.current_url.lower()
                    if any(j in url for j in self.JUNK_URLS):
                        logger.info(f"    🗑 Closing popup: {url[:60]}")
                        self.driver.close()
                except Exception:
                    pass
            self.driver.switch_to.window(current)
        except Exception:
            pass

        except Exception as e:
            logger.warning(f"Popup dismiss error: {str(e)[:80]}")

    # ------------------------------------------------------------------
    # NAV HELPERS
    # ------------------------------------------------------------------
    def _safe_get(self, url, retries=3):
        for attempt in range(1, retries + 1):
            try:
                if not self._driver_alive():
                    if not self._recover():
                        return False
                self.driver.get(url)
                WebDriverWait(self.driver, self.page_wait).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # Dismiss any popups that opened during navigation
                self._dismiss_popups()
                return True
            except TimeoutException:
                logger.warning(f"Timeout {url} (attempt {attempt})")
                self._dismiss_popups()
                time.sleep(3)
            except WebDriverException as e:
                logger.warning(f"WebDriver nav error (attempt {attempt}): {str(e)[:100]}")
                if attempt < retries:
                    self._recover()
                    time.sleep(2)
        return False

    def _wait_for_page(self, timeout=20, min_text=50):
        """Wait until page body has at least min_text characters."""
        end = time.time() + timeout
        while time.time() < end:
            try:
                body = self.driver.find_element(By.TAG_NAME, "body")
                if len(body.text.strip()) > min_text:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

    def _close_extra_tabs(self):
        try:
            handles = self.driver.window_handles
            if len(handles) > 1:
                main = handles[0]
                for h in handles[1:]:
                    self.driver.switch_to.window(h)
                    self.driver.close()
                self.driver.switch_to.window(main)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # SEARCH & CAPTCHA
    # ------------------------------------------------------------------
    def _search_category(self, cat_name, cat_value, keyword="cancer"):
        logger.info(f"🔎 Searching: {cat_name}")
        if not self._safe_get(self.CTRI_URL):
            return False
        try:
            kw = self.wait.until(EC.presence_of_element_located((By.NAME, "searchword")))
            kw.clear()
            kw.send_keys(keyword)
            Select(self.driver.find_element(By.NAME, "searchtype")).select_by_value(cat_value)

            banner(f"CAPTCHA — {cat_name}")
            print("  1. Enter the CAPTCHA in the browser")
            print("  2. Click the green SEARCH button")
            print("  3. Wait for the results page to fully load")
            input("\n  >>> Press ENTER when results are loaded... ")
            time.sleep(2)

            if "pubview2.php" in self.driver.current_url:
                logger.info("✓ Results loaded")
                return True
            time.sleep(1)
            if "pubview2.php" in self.driver.current_url:
                return True
            input("  >>> Results not detected. Press ENTER if results are showing... ")
            return True
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return False

    # ------------------------------------------------------------------
    # TRIAL URL EXTRACTION
    # ------------------------------------------------------------------
    def _get_trial_urls(self):
        logger.info("📋 Extracting trial URLs...")
        urls = []
        try:
            time.sleep(2)
            src = self.driver.page_source
            for m in re.findall(r"newwin2\('([^']+)'\s*(?:,\s*'[^']*')?\)", src):
                if "pmaindet2.php" in m:
                    full = f"https://ctri.nic.in/Clinicaltrials/{m}"
                    if full not in urls:
                        urls.append(full)
            logger.info(f"✓ Found {len(urls)} trial URLs")
        except Exception as e:
            logger.error(f"URL extraction error: {e}")
        return urls

    # ------------------------------------------------------------------
    # PDF DOWNLOAD & PARSE  (v2 — section-aware extraction)
    # ------------------------------------------------------------------

    # Section markers that appear in PDF text to delimit sections
    SECTION_MARKERS = [
        "Details of Principal Investigator",
        "Details Contact Person (Scientific Query)",
        "Details Contact Person\n(Scientific Query)",
        "Details Contact Person (Public Query)",
        "Details Contact Person\n(Public Query)",
        "Source of Monetary",
        "Primary Sponsor",
        "Details of Secondary Sponsor",
        "Countries of Recruitment",
        "Sites of Study",
        "Details of Ethics Committee",
        "Regulatory Clearance Status",
        "Health Condition",
        "Intervention / Comparator",
        "Inclusion Criteria",
        "Exclusion Criteria",
        "Method of Generating",
        "Blinding/Masking",
        "Primary Outcome",
        "Secondary Outcome",
        "Target Sample Size",
        "Phase of Trial",
        "Date of First Enrollment",
        "Date of Study Completion",
        "Estimated Duration",
        "Recruitment Status",
        "Publication Details",
        "Brief Summary",
    ]

    # Single-line label → FIXED_COLUMNS key mapping
    LINE_FIELD_MAP = {
        "Last Modified On": "Last_Modified_Date",
        "Post Graduate Thesis": "Post_Graduate_Thesis",
        "Type of Trial": "Type_of_Trial",
        "Type of Study": "Type_of_Study",
        "Study Design": "Study_Design",
        "Phase of Trial": "Phase_of_Trial",
        "Blinding/Masking": "Blinding_Masking",
        "Method of Concealment": "Method_of_Concealment",
        "Recruitment Status (Global)": "Recruitment_Status_Global",
        "Recruitment Status (India)": "Recruitment_Status_India",
        "Estimated Duration of Trial": "Estimated_Duration",
        "Trial Acronym": "Trial_Acronym",
    }

    def _clean_text(self, text):
        """Decode HTML entities, strip HTML tags, and normalize special characters."""
        if not text:
            return ""
        
        # 0. Strip HTML tags (like <br/>, <b>, &nbsp;) and code-like snippets
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'var\s+\w+\s*=.*?;', '', text) # Simple JS removal
        
        # 1. Unescape HTML entities
        text = html_module.unescape(text)
        
        # 2. Fix common mis-encoded UTF-8 as CP1252 artifacts (noise)
        replacements = {
            r'â€¢': '•', r'â€œ': '"', r'â€\?': '"', r'â€': "'", r'â€”': '—',
            r'â€“': '–', r'â€™': "'", r'â‰¥': '≥', r'Ã—': '×', r'Â§': '§',
            r'Â': ' ', r'': '≤', r'': '>', r'\?+': ' ',
        }
        for pattern, replacement in replacements.items():
            text = re.sub(pattern, replacement, text)
            
        # 3. Clean numeric entities
        text = re.sub(r'&amp;#(\d+);', lambda m: chr(int(m.group(1))) if int(m.group(1)) < 0x110000 else ' ', text)
        text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))) if int(m.group(1)) < 0x110000 else ' ', text)
        
        # 4. Remove system noise
        text = re.sub(r'Powered by TCPDF.*', '', text)
        
        # 5. Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _remove_noise(self, text, field_name=None):
        """Robust noise removal for specific fields and common placeholders."""
        if not text or not isinstance(text, str):
            return text
            
        # 1. Strip common placeholders (case-insensitive)
        placeholders = [
            r'\bNot Applicable\b', r'\bN/A\b', r'\bNIL\b', r'\bNone\b', 
            r'\bNone yet\b', r'^NA$', r'\bTBA\b', r'\bTo be announced\b'
        ]
        for p in placeholders:
            text = re.sub(p, '', text, flags=re.I).strip()
            
        # 2. Field-specific noise removal
        if field_name in ("Age_From", "Age_To"):
            text = re.sub(r'Year\(s\)', '', text, flags=re.I)
            text = re.sub(r'\.00', '', text)
            text = re.sub(r'[^0-9\.\-]', '', text) # Keep only numbers/dots/hyphens
        
        elif field_name == "Gender":
            text = re.sub(r'Year\(s\)', '', text, flags=re.I).strip()
            
        elif field_name in ("Inclusion_Criteria", "Exclusion_Criteria"):
            # Strip leading bullets: "1. ", "a) ", "• ", "- ", "* "
            text = re.sub(r'^\s*([a-zA-Z0-9][\.\)]|[\-•\*])\s+', '', text)
            # Remove repeated markers often found in wrapped text
            text = re.sub(r'\s+([a-zA-Z0-9][\.\)]|[\-•\*])\s+', ' ', text)

        # 3. Final whitespace cleanup
        text = re.sub(r'\s+', ' ', text).strip()
        # If it's just punctuation, clear it
        if text and all(c in '.,:;|- ' for c in text):
            return ""
        return text

    def _download_and_parse_pdf(self):
        """Download PDF and extract ALL fields using section-aware parsing."""
        data = {}
        try:
            self._sync_cookies_to_requests()
            headers = {
                "Referer": self.driver.current_url,
                "User-Agent": self.driver.execute_script("return navigator.userAgent;"),
            }
            logger.info("    ⬇ Downloading PDF...")
            resp = self.http_session.get(self.PDF_URL, headers=headers, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"    PDF download HTTP {resp.status_code}")
                return data
            content_type = resp.headers.get("Content-Type", "")
            if "pdf" not in content_type.lower():
                # Check if response is HTML (login page, error page, etc.)
                if "html" in content_type.lower() or resp.content[:100].strip().startswith(b'<'):
                    logger.warning(f"    PDF response not a PDF (content-type: {content_type})")
                    return data
                # Unknown content type but might still be a PDF — check magic bytes
                if not resp.content[:5].startswith(b'%PDF'):
                    logger.warning(f"    PDF response not a PDF (no PDF magic bytes, content-type: {content_type})")
                    return data
            logger.info(f"    ✓ PDF downloaded ({len(resp.content) // 1024} KB)")

            pdf_bytes = io.BytesIO(resp.content)
            with pdfplumber.open(pdf_bytes) as pdf:
                logger.info(f"    📄 PDF has {len(pdf.pages)} pages")

                # STEP 1: Extract raw text from every page
                all_lines = []
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    for line in text.split("\n"):
                        all_lines.append(line.strip())

                # STEP 2: Join wrapped lines
                joined = self._join_wrapped_lines(all_lines)

                # STEP 3: Section-aware text extraction
                self._parse_sections(joined, data)

                # STEP 4: Extract tables for structured data
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        if table:
                            self._parse_table_v2(table, data)

                # STEP 5: Post-process
                self._post_process_v2(data)

        except requests.exceptions.Timeout:
            logger.warning("    PDF download timed out")
        except Exception as e:
            logger.error(f"    PDF parse error: {str(e)[:150]}")
        return data

    def _join_wrapped_lines(self, lines):
        """Join lines that are split across PDF line breaks."""
        joined = []
        i = 0
        while i < len(lines):
            line = lines[i]
            if i + 1 < len(lines):
                nxt = lines[i + 1]
                if nxt.startswith("(India)") or nxt.startswith("(Global)"):
                    for prefix in ("Date of First Enrollment ",
                                   "Date of Study Completion ",
                                   "Recruitment Status "):
                        if line.startswith(prefix):
                            value_part = line[len(prefix):]
                            line = prefix + nxt + " " + value_part
                            break
                    else:
                        line = line + " " + nxt
                    i += 1
                elif nxt == "Random Sequence" and "Method of Generating" in line:
                    val = line.replace("Method of Generating", "").strip()
                    line = "Method of Generating Random Sequence " + val
                    i += 1
                elif line.startswith("Source of Monetary") and nxt == "Material Support":
                    line = line + " " + nxt
                    i += 1
                elif nxt == "(Scientific Query)" or nxt.startswith("(Scientific"):
                    line = line + " " + nxt
                    i += 1
                elif nxt == "(Public Query)" or nxt.startswith("(Public"):
                    line = line + " " + nxt
                    i += 1
            joined.append(line)
            i += 1
        return joined

    def _detect_section(self, line):
        """Detect which section a line belongs to. Returns section name or None."""
        ll = line.lower()
        if "details of principal investigator" in ll:
            return "pi"
        if "details contact person" in ll and "scientific" in ll:
            return "scientific"
        if "details contact person" in ll and "public" in ll:
            return "public"
        if "source of monetary" in ll:
            return "funding"
        if "primary sponsor" in ll and ("details" in ll or line.startswith("Primary Sponsor")):
            return "sponsor"
        if "details of secondary sponsor" in ll:
            return "secondary_sponsor"
        if "countries of recruitment" in ll:
            return "countries"
        if "sites of study" in ll:
            return "sites"
        if "details of ethics committee" in ll:
            return "ethics"
        if "regulatory clearance" in ll:
            return "dcgi"
        if "health condition" in ll or ("health type" in ll and "condition" in ll):
            return "health"
        if "intervention / comparator" in ll or "intervention/comparator" in ll:
            return "intervention"
        if "inclusion criteria" in ll:
            return "inclusion"
        if "exclusion criteria" in ll:
            return "exclusion"
        if "primary outcome" in ll:
            return "primary_outcome"
        if "secondary outcome" in ll:
            return "secondary_outcome"
        if "target sample size" in ll:
            return "sample_size"
        if "brief summary" in ll:
            return "summary"
        if "publication details" in ll:
            return "publication"
        return None

    def _parse_sections(self, lines, data):
        """Parse all text lines with section awareness for contact attribution."""
        section = "header"
        # Contact field labels and which section prefix to use
        CONTACT_LABELS = {"Name", "Designation", "Affiliation", "Address", "Phone", "Fax", "Email"}
        CONTACT_PREFIX = {"pi": "PI", "scientific": "Scientific_Contact", "public": "Public_Contact"}
        # Track multi-line fields
        multiline_key = None
        multiline_parts = []

        uncategorized_lines = []

        def flush_multiline():
            nonlocal multiline_key, multiline_parts
            if multiline_key and multiline_parts:
                val = " ".join(multiline_parts)
                # Cleanup: if the value contains labels of other sections, strip them
                # This prevents "bleeding" of labels into the captured values
                stop_words = ["Scientific Title of Study", "Primary Outcome", "Secondary Outcome", "Inclusion Criteria", "Exclusion Criteria", "Secondary IDs if Any", "Brief Summary", "Details of Principal"]
                for sw in stop_words:
                    if sw in val:
                        val = val.split(sw)[0].strip()
                
                cleaned_val = self._clean_text(val)
                if cleaned_val and not data.get(multiline_key):
                    data[multiline_key] = cleaned_val
            multiline_key = None
            multiline_parts = []

        # We'll use this to track which lines were "captured" by specific logic
        # and store everything else in Uncategorized_Data.
        
        for i, line in enumerate(lines):
            line_handled = False
            if not line:
                continue

            # Check for section change
            new_section = self._detect_section(line)
            if new_section:
                flush_multiline()
                section = new_section
                line_handled = True
                # Some section header lines also contain data after the header
                if section == "summary":
                    val = line[len("Brief Summary"):].strip() if line.startswith("Brief Summary") else ""
                    if val:
                        multiline_key = "Brief_Summary"
                        multiline_parts = [val]
                    else:
                        multiline_key = "Brief_Summary"
                        multiline_parts = []
                elif section == "publication":
                    val = line[len("Publication Details"):].strip() if line.startswith("Publication Details") else ""
                    if val:
                        multiline_key = "Publication_Details"
                        multiline_parts = [val]
                    else:
                        multiline_key = "Publication_Details"
                        multiline_parts = []
                elif section == "sample_size":
                    if "Total Sample Size:" in line:
                        m = re.search(r'Total Sample Size:\s*(\S+)', line)
                        if m:
                            data["Sample_Size_Total"] = m.group(1)
                    if "Sample Size from India:" in line:
                        m = re.search(r'Sample Size from India:\s*(\S+)', line)
                        if m:
                            data["Sample_Size_India"] = m.group(1)
                continue

            # If collecting multiline, keep going
            if multiline_key:
                line_handled = True
                MULTILINE_FLUSH_TRIGGERS = [
                    "Secondary IDs if Any", "Secondary IDs", "Secondary ID",
                    "Details of Principal", "Details Contact Person",
                    "Source of Monetary", "Primary Sponsor",
                    "Countries of Recruitment", "Sites of Study",
                    "Target Sample Size", "Phase of Trial",
                    "Powered by TCPDF",
                ]
                should_flush = any(line.startswith(lbl) for lbl in self.LINE_FIELD_MAP) or \
                               self._detect_section(line) is not None or \
                               any(trigger in line for trigger in MULTILINE_FLUSH_TRIGGERS)
                if should_flush:
                    flush_multiline()
                    # fall through to parse this line normally
                    line_handled = False
                else:
                    if "TCPDF" not in line and "tcpdf.org" not in line:
                        multiline_parts.append(line)
                    continue

            # === HEADER SECTION (CTRI Number area) ===
            if line.startswith("CTRI Number"):
                line_handled = True
                val = line[len("CTRI Number"):].strip()
                data["CTRI_Number"] = val
                m = re.search(r'Registered on:\s*([\d/]+)', val)
                if m:
                    data["Registration_Date"] = m.group(1)
                continue

            # === Single-line mapped fields ===
            for label, col in self.LINE_FIELD_MAP.items():
                if line.startswith(label):
                    line_handled = True
                    val = line[len(label):].strip()
                    if val and not data.get(col):
                        data[col] = val
                    break
            if line_handled:
                continue

            # === Multi-line title fields ===
            if "Public Title of Study" in line:
                line_handled = True
                multiline_key = "Public_Title"
                idx = line.find("Public Title of Study")
                multiline_parts = [line[idx + len("Public Title of Study"):].strip()]
                continue
            if "Scientific Title" in line:
                line_handled = True
                flush_multiline()
                multiline_key = "Scientific_Title"
                match = re.search(r'Scientific Title(?:\s+of\s+Study)?\s*(.*)', line, re.I)
                content = match.group(1).strip() if match else ""
                stop_words = ["Secondary IDs if Any", "Details of Principal", "Secondary ID"]
                for sw in stop_words:
                    if sw in content:
                        content = content.split(sw)[0].strip()
                multiline_parts = [content]
                continue

            # === Contact person fields (section-aware) ===
            if section in CONTACT_PREFIX:
                prefix = CONTACT_PREFIX[section]
                for label in CONTACT_LABELS:
                    if line.startswith(label + " ") or line == label:
                        line_handled = True
                        val = line[len(label):].strip() if line.startswith(label + " ") else ""
                        col = f"{prefix}_{label}"
                        if val and not data.get(col):
                            data[col] = val
                        break
                if line_handled:
                    continue

            # === Sponsor section ===
            if section == "sponsor":
                if line.startswith("Name "):
                    line_handled = True
                    val = line[5:].strip()
                    if val and not data.get("Primary_Sponsor_Name"):
                        data["Primary_Sponsor_Name"] = val
                elif line.startswith("Address "):
                    line_handled = True
                    val = line[8:].strip()
                    if val and not data.get("Primary_Sponsor_Address"):
                        data["Primary_Sponsor_Address"] = val
                elif line.startswith("Type of Sponsor"):
                    line_handled = True
                    val = line[len("Type of Sponsor"):].strip()
                    if val and not data.get("Primary_Sponsor_Type"):
                        data["Primary_Sponsor_Type"] = val
                if line_handled: continue

            # === Secondary Sponsor ===
            if section == "secondary_sponsor":
                if line.startswith("Name ") or line.startswith("Details "):
                    line_handled = True
                    val = line.split(" ", 1)[1].strip() if " " in line else ""
                    if val:
                        existing = data.get("Secondary_Sponsors", "")
                        data["Secondary_Sponsors"] = f"{existing} | {val}" if existing else val
                if line_handled: continue

            # === Funding ===
            if section == "funding":
                if line.startswith("Source ") and "Monetary" not in line and "Material" not in line:
                    line_handled = True
                    val = line[7:].strip()
                    if val:
                        existing = data.get("Source_of_Funding", "")
                        data["Source_of_Funding"] = f"{existing} | {val}" if existing else val
                if line_handled: continue

            # === Countries ===
            if section == "countries":
                if line.startswith("List of Countries"):
                    line_handled = True
                    val = line[len("List of Countries"):].strip()
                    if val:
                        data["Countries_of_Recruitment"] = val
                elif not any(line.startswith(x) for x in ["Countries", "Principal", "Site"]):
                    line_handled = True
                    existing = data.get("Countries_of_Recruitment", "")
                    if line and line not in existing:
                        data["Countries_of_Recruitment"] = f"{existing}, {line}" if existing else line
                if line_handled: continue

            # === Inclusion Criteria ===
            if section == "inclusion":
                if line.startswith("Age From"):
                    line_handled = True
                    m = re.search(r'Age From\s+(.*)', line)
                    if m and not data.get("Age_From"):
                        data["Age_From"] = m.group(1).strip()
                elif line.startswith("Age To"):
                    line_handled = True
                    m = re.search(r'Age To\s+(.*)', line)
                    if m and not data.get("Age_To"):
                        data["Age_To"] = m.group(1).strip()
                elif line.startswith("Gender"):
                    line_handled = True
                    m = re.search(r'Gender\s+(.*)', line)
                    if m and not data.get("Gender"):
                        data["Gender"] = m.group(1).strip()
                elif line.startswith("Details ") or (not line.startswith("Inclusion") and not line.startswith("Exclusion")):
                    line_handled = True
                    val = line[8:].strip() if line.startswith("Details ") else line
                    if val and "TCPDF" not in val:
                        existing = data.get("Inclusion_Criteria", "")
                        data["Inclusion_Criteria"] = f"{existing} {val}" if existing else val
                if line_handled: continue

            # === Exclusion Criteria ===
            if section == "exclusion":
                if line.startswith("Details "):
                    line_handled = True
                    val = self._clean_text(line[8:].strip())
                    if val:
                        existing = data.get("Exclusion_Criteria", "")
                        data["Exclusion_Criteria"] = f"{existing} {val}" if existing else val
                elif not line.startswith("Exclusion") and not line.startswith("Method of"):
                    line_handled = True
                    val = self._clean_text(line)
                    if val and "TCPDF" not in val:
                        existing = data.get("Exclusion_Criteria", "")
                        data["Exclusion_Criteria"] = f"{existing} {val}" if existing else val
                if line_handled: continue

            # === Primary / Secondary Outcome ===
            if section == "primary_outcome":
                if line != "Outcome Timepoints" and not line.startswith("Outcome "):
                    line_handled = True
                    existing = data.get("Primary_Outcome", "")
                    data["Primary_Outcome"] = f"{existing} || {line}" if existing else line
                if line_handled: continue

            if section == "secondary_outcome":
                if line != "Outcome Timepoints" and not line.startswith("Outcome "):
                    line_handled = True
                    existing = data.get("Secondary_Outcome", "")
                    data["Secondary_Outcome"] = f"{existing} || {line}" if existing else line
                if line_handled: continue

            # === DCGI ===
            if section == "dcgi":
                if line.startswith("Status "):
                    line_handled = True
                    val = line[7:].strip()
                    if val and not data.get("DCGI_Status"):
                        data["DCGI_Status"] = val
                if line_handled: continue

            # === Health Condition ===
            if section == "health":
                if "Health Type" not in line and "Condition" != line.strip():
                    line_handled = True
                    val = line.strip()
                    if val and len(val) > 2:
                        existing = data.get("Health_Condition", "")
                        if val not in existing:
                            data["Health_Condition"] = f"{existing} | {val}" if existing else val
                if line_handled: continue

            # === Date fields & Randomization ===
            if line.startswith("Date of First Enrollment"):
                line_handled = True
            elif line.startswith("Date of Study Completion"):
                line_handled = True
            elif line.startswith("Method of Generating Random Sequence"):
                line_handled = True
            elif line.startswith("Study Design"):
                line_handled = True
            
            # Final check: if not handled, add to uncategorized
            if not line_handled:
                # Filter out pure boilerplate/noise
                if not any(x in line for x in ["TCPDF", "tcpdf.org", "Page ", "Powered by"]):
                    cleaned = line.strip()
                    if cleaned and len(cleaned) > 1:
                        uncategorized_lines.append(cleaned)

        # Store uncategorized data
        if uncategorized_lines:
            data["Uncategorized_Data"] = " | ".join(uncategorized_lines)

        # Flush any remaining multiline
        flush_multiline()
        # Clean inclusion/exclusion
        for key in ("Inclusion_Criteria", "Exclusion_Criteria"):
            if data.get(key):
                data[key] = self._clean_text(data[key])

    def _parse_table_v2(self, table, data):
        """Parse a PDF table — handles Sites, Intervention, Ethics, Outcomes, etc."""
        if not table or not table[0]:
            return
        ncols = len(table[0])
        hdr = " ".join([(c or "").strip().lower() for c in table[0]])

        # === Sites of Study (3+ cols) ===
        if "principal investigator" in hdr and "site name" in hdr:
            sites = []
            for row in table[1:]:
                cells = [(c or "").strip().replace("\n", " ") for c in row]
                if any(cells):
                    pi = cells[0] if len(cells) > 0 else ""
                    site = cells[1] if len(cells) > 1 else ""
                    addr = cells[2] if len(cells) > 2 else ""
                    contact = cells[3] if len(cells) > 3 else ""
                    sites.append(f"{pi} @ {site}, {addr} ({contact})")
            if sites:
                existing = data.get("Sites_of_Study", "")
                new_val = " || ".join(sites)
                data["Sites_of_Study"] = f"{existing} || {new_val}" if existing else new_val
            return

        # === Intervention / Comparator (3 cols: Type, Name, Details) ===
        if "type" in hdr and "name" in hdr and "details" in hdr:
            for row in table[1:]:
                cells = [(c or "").strip().replace("\n", " ") for c in row]
                if len(cells) >= 3 and any(cells):
                    itype = cells[0].lower()
                    iname = cells[1]
                    idetails = self._clean_text(cells[2])
                    if "intervention" in itype:
                        if not data.get("Intervention_Name"):
                            data["Intervention_Name"] = iname
                            data["Intervention_Details"] = idetails
                        else:
                            data["Intervention_Name"] += f" | {iname}"
                            data["Intervention_Details"] += f" | {idetails}"
                    elif "comparator" in itype:
                        if not data.get("Comparator_Name"):
                            data["Comparator_Name"] = iname
                            data["Comparator_Details"] = idetails
                        else:
                            data["Comparator_Name"] += f" | {iname}"
                            data["Comparator_Details"] += f" | {idetails}"
            return

        # === Ethics Committee (Name | Approval Status) ===
        if "approval" in hdr or "approval status" in hdr:
            entries = []
            for row in table[1:]:
                cells = [(c or "").strip().replace("\n", " ") for c in row]
                if len(cells) >= 2 and cells[0]:
                    entries.append(f"{cells[0]}: {cells[1]}")
            if entries:
                existing = data.get("Ethics_Committee", "")
                new_val = " || ".join(entries)
                data["Ethics_Committee"] = f"{existing} || {new_val}" if existing else new_val
            return

        # === Outcome table ===
        if "outcome" in hdr and "timepoints" in hdr:
            outcomes = []
            timepoints = []
            for row in table[1:]:
                cells = [(c or "").strip().replace("\n", " ") for c in row]
                o = cells[0] if len(cells) > 0 else ""
                t = cells[1] if len(cells) > 1 else ""
                if o and o.lower() != "outcome":
                    outcomes.append(o)
                if t and t.lower() != "timepoints":
                    timepoints.append(t)
            if outcomes:
                existing_o = data.get("Primary_Outcome", "")
                new_o = " || ".join(outcomes)
                if not existing_o:
                    data["Primary_Outcome"] = new_o
                else:
                    # Might be secondary outcome table
                    existing_s = data.get("Secondary_Outcome", "")
                    data["Secondary_Outcome"] = f"{existing_s} || {new_o}" if existing_s else new_o
            if timepoints:
                tp_str = " || ".join(timepoints)
                if not data.get("Primary_Outcome_Timepoints"):
                    data["Primary_Outcome_Timepoints"] = tp_str
                else:
                    data["Secondary_Outcome_Timepoints"] = tp_str
            return

        # === Health Type table ===
        if "health type" in hdr:
            for row in table:
                cells = [(c or "").strip().replace("\n", " ") for c in row]
                if len(cells) >= 2 and cells[1] and cells[0].lower() != "health type":
                    existing = data.get("Health_Condition", "")
                    val = cells[1]
                    if val not in existing:
                        data["Health_Condition"] = f"{existing} | {val}" if existing else val
            return

        # === Secondary ID table ===
        if "secondary id" in hdr or "identifier" in hdr:
            for row in table:
                cells = [(c or "").strip().replace("\n", " ") for c in row]
                if len(cells) >= 2 and cells[0] and "secondary id" not in cells[0].lower():
                    entry = f"{cells[0]} ({cells[1]})"
                    existing = data.get("Secondary_IDs", "")
                    if entry not in existing:
                        data["Secondary_IDs"] = f"{existing} | {entry}" if existing else entry
            return

        # === 1-col tables (Source, Countries, DCGI status) ===
        if ncols == 1:
            for row in table:
                cell = (row[0] or "").strip().replace("\n", " ")
                if not cell or len(cell) < 3:
                    continue
                if cell.startswith("Source ") and "Monetary" not in cell and "Material" not in cell:
                    val = cell[7:].strip()
                    if val:
                        existing = data.get("Source_of_Funding", "")
                        if val not in existing:
                            data["Source_of_Funding"] = f"{existing} | {val}" if existing else val
                elif "List of Countries" in cell:
                    m = re.search(r'List of Countries\s+(.*)', cell)
                    if m:
                        existing = data.get("Countries_of_Recruitment", "")
                        val = m.group(1).strip()
                        if val and val not in existing:
                            data["Countries_of_Recruitment"] = f"{existing}, {val}" if existing else val
                elif cell.startswith("Status ") and "Recruitment" not in cell:
                    val = cell[7:].strip()
                    if val and not data.get("DCGI_Status"):
                        data["DCGI_Status"] = val
            return

        # === Generic 2-col contact tables (with section awareness) ===
        CONTACT_FIELDS = {"Name", "Designation", "Affiliation", "Address", "Phone", "Fax", "Email"}
        SECTION_HEADERS = {
            "details of principal investigator": "pi",
            "details contact person (scientific query)": "scientific",
            "details contact person (public query)": "public",
        }
        # Check if this table is a section header
        for sh, sec in SECTION_HEADERS.items():
            if sh in hdr:
                self._current_section = sec
                break

        if self._current_section in ("pi", "scientific", "public"):
            prefix_map = {"pi": "PI", "scientific": "Scientific_Contact", "public": "Public_Contact"}
            prefix = prefix_map[self._current_section]
            for row in table[1:] if len(table) > 1 else []:
                if not row or len(row) < 2:
                    continue
                label = re.sub(r'\s+', ' ', (row[0] or "")).strip()
                value = re.sub(r'\s+', ' ', (row[1] or "")).strip()
                if not label or not value or label == value:
                    continue
                if label in CONTACT_FIELDS:
                    if value in CONTACT_FIELDS:
                        continue
                    col = f"{prefix}_{label}"
                    if not data.get(col):
                        data[col] = value

        # === Primary Sponsor table ===
        if "primary sponsor" in hdr:
            for row in table[1:] if len(table) > 1 else []:
                if not row or len(row) < 2:
                    continue
                label = re.sub(r'\s+', ' ', (row[0] or "")).strip()
                value = re.sub(r'\s+', ' ', (row[1] or "")).strip()
                if label == "Name" and value and not data.get("Primary_Sponsor_Name"):
                    data["Primary_Sponsor_Name"] = value
                elif label == "Address" and value and not data.get("Primary_Sponsor_Address"):
                    data["Primary_Sponsor_Address"] = value
                elif label == "Type of Sponsor" and value and not data.get("Primary_Sponsor_Type"):
                    data["Primary_Sponsor_Type"] = value

        # === Secondary Sponsor table ===
        if "secondary sponsor" in hdr:
            for row in table[1:] if len(table) > 1 else []:
                if not row or len(row) < 2:
                    continue
                label = re.sub(r'\s+', ' ', (row[0] or "")).strip()
                value = re.sub(r'\s+', ' ', (row[1] or "")).strip()
                if label == "Name" and value:
                    existing = data.get("Secondary_Sponsors", "")
                    data["Secondary_Sponsors"] = f"{existing} | {value}" if existing else value

    def _post_process_v2(self, data):
        """Clean fields and derive oncology tags."""
        # Clean CTRI IDs
        if "Secondary_IDs" in data:
            ids = [i.strip() for i in data["Secondary_IDs"].split("|")]
            for i, val in enumerate(ids):
                m = re.search(r'(CTRI/\S+)', val)
                if m:
                    ids[i] = m.group(1).rstrip(']').rstrip(')')
            data["Secondary_IDs"] = " | ".join(list(set(ids)))

        ctri = data.get("CTRI_Number", "")
        if ctri:
            m2 = re.search(r'(CTRI/\S+)', ctri)
            if m2:
                data["CTRI_Number"] = m2.group(1).rstrip(']').rstrip(')')

        # Apply noise removal to ALL fields
        for key in list(data.keys()):
            data[key] = self._remove_noise(data[key], field_name=key)

        # Trial Acronym from titles
        pt = data.get("Public_Title", "")
        st = data.get("Scientific_Title", "")
        acronym_match = re.search(r'([A-Z][A-Za-z0-9\-]{2,15}(?:\s+\d+)?)\s*:', pt)
        if acronym_match and not data.get("Trial_Acronym"):
            data["Trial_Acronym"] = acronym_match.group(1)

        # Derived Oncology Fields
        # 1. Combine all logic columns for a thorough scan
        text_parts = [
            data.get("Public_Title", ""),
            data.get("Scientific_Title", ""),
            data.get("Health_Condition", ""),
            data.get("Inclusion_Criteria", ""),
            data.get("Exclusion_Criteria", ""),
            data.get("Brief_Summary", ""),
            data.get("Intervention_Name", ""),
            data.get("Intervention_Details", ""),
            data.get("Uncategorized_Data", "")
        ]
        search_text = " ".join([str(p).strip() for p in text_parts if p])
        
        if search_text:
            # Targeted Therapy to Mutation Mapping
            DRUG_MAP = {
                "EGFR": ["Osimertinib", "Gefitinib", "Erlotinib", "Afatinib", "Dacomitinib", "Cetuximab", "Panitumumab", "Amivantamab", "Tagrisso", "Iressa", "Tarceva"],
                "ALK": ["Alectinib", "Brigatinib", "Ceritinib", "Crizotinib", "Lorlatinib", "Ensartinib", "Alecensa", "Alunbrig"],
                "HER2": ["Trastuzumab", "Pertuzumab", "Lapatinib", "Neratinib", "Tucatinib", "Enhertu", "Herceptin", "Perjeta", "Tykerb"],
                "BRAF": ["Vemurafenib", "Dabrafenib", "Encorafenib", "Zelboraf", "Tafinlar"],
                "MEK": ["Trametinib", "Cobimetinib", "Binimetinib", "Selumetinib", "Mekinist"],
                "ROS1": ["Entrectinib", "Repotrectinib", "Crizotinib", "Rozlytrek"],
                "KRAS": ["Sotorasib", "Adagrasib", "Lumakras"],
                "BRCA": ["Olaparib", "Niraparib", "Rucaparib", "Talazoparib", "Lynparza", "Zejula"],
                "PD-L1/PD-1": ["Pembrolizumab", "Nivolumab", "Atezolizumab", "Durvalumab", "Avelumab", "Cemiplimab", "Keytruda", "Opdivo", "Tecentriq", "Imfinzi"],
                "RET": ["Selpercatinib", "Pralsetinib", "Retevmo"],
                "MET": ["Capmatinib", "Tepotinib", "Tabrecta"],
                "FGFR": ["Erdafitinib", "Pemigatinib", "Balversa"],
            }
            
            GENES = ["EGFR", "KRAS", "NRAS", "BRAF", "ALK", "ROS1", "HER2", "MET", "RET", 
                    "NTRK", "PIK3CA", "BRCA1", "BRCA2", "PTEN", "FGFR", "IDH1", "IDH2", "KIT", 
                    "TP53", "MMR", "MSI", "PD-L1", "PD-1", "TMB", "TROP2"]

            KEYWORDS = ["mutation", "mutated", "rearrangement", "amplification", "deletion", 
                        "insertion", "wild-type", "variant", "translocation", "fusion", 
                        "alteration", "positive", "negative", "overexpression"]

            found_info = []

            # A. Drug Inference (from intervention text)
            itex = (str(data.get("Intervention_Name", "")) + " " + str(data.get("Intervention_Details", ""))).lower()
            for gene, drugs in DRUG_MAP.items():
                if any(d.lower() in itex for d in drugs):
                    found_info.append(f"{gene} Positive (inferred from targeted drug)")

            # B. Deep Scan Sentences
            sentences = re.split(r'\. |\n|; | (?=[A-Z][a-z]+ \b)', search_text)
            for sent in sentences:
                sent = re.sub(r'\s+', ' ', sent).strip()
                if len(sent) < 15: continue
                
                # Markers
                if re.search(r'\b(triple\s*negative|TNBC)\b', sent, re.I):
                    found_info.append("Triple Negative (ER-/PR-/HER2-)")
                    continue
                
                marker_found = False
                for g in set(GENES) | set(DRUG_MAP.keys()):
                    if re.search(rf'\b{re.escape(g)}\b', sent, re.I):
                        if any(re.search(rf'\b{re.escape(kw)}\b', sent, re.I) for kw in KEYWORDS) or \
                           re.search(r'\b[A-Z]\d+[A-Z]\b', sent) or re.search(r'\bexon\s+\d+\b', sent, re.I):
                            found_info.append(sent[:200])
                            marker_found = True
                            break
                
                if not marker_found and re.search(r'\b(ER|PR|HR)\s*(?:positive|negative|\+|\-)\b', sent, re.I):
                    found_info.append(sent[:150])

            if found_info:
                uniq = []
                seen = set()
                for f in found_info:
                    if f.lower() not in seen:
                        uniq.append(f)
                        seen.add(f.lower())
                data["Mutation_Inclusion_Criteria"] = " | ".join(uniq[:3])

            # 2. Line of Therapy
            line_match = re.search(r'(\b\d(?:st|nd|rd|th)\s+line|first\s+line|second\s+line|third\s+line|front\s*line)\b', search_text, re.I)
            if line_match:
                data["Line_of_Therapy"] = line_match.group(1).strip().lower()

            # 3. Stage Requirements
            stage_match = re.search(r'(stage\s+[IVXB1234]{1,5}|metastatic|locally\s+advanced|recurrent|advanced)', search_text, re.I)
            if stage_match:
                data["Stage_Requirements"] = stage_match.group(1).strip().lower()

            # 4. ECOG Performance Status
            ecog_match = re.search(r'(ECOG\s*(?:performance status)?\s*(?:score)?\s*(?:of)?\s*(?:<=?|>=?|=)?\s*[0-4](?:\s*(?:-|/|to)\s*[0-4])?)', search_text, re.I)
            if ecog_match:
                data["ECOG_Performance_Status"] = ecog_match.group(1).strip()

            # 5. Prior Treatment
            prior_match = re.search(r'(prior\s+[^.]{5,30}|failure\s+of\s+[^.]{5,30}|previously\s+treated|treatment\s+naive)', search_text, re.I)
            if prior_match:
                data["Prior_Treatment_Requirements"] = prior_match.group(1).strip()

        # Apply noise removal to ALL fields
        for key in list(data.keys()):
            data[key] = self._remove_noise(data[key])

    def _remove_noise(self, text):
        """Strip HTML tags, PDF boilerplate, and collapse whitespace."""
        if not text or not isinstance(text, str):
            return ""
        
        # 1. Unescape HTML and strip tags
        text = html_module.unescape(text)
        text = re.sub(r'<[^>]*>', ' ', text)
        
        # 2. PDF Noise Patterns
        noise_patterns = [
            r'Page \d+ of \d+', r'List of Countries', r'Site study', r'Site Study',
            r'Status Recruitment', r'Details of Principal Investigator',
            r'Details contact person', r'Scientific Query', r'Public Query',
            r'Ethics Committee', r'Primary Sponsor', r'Secondary Sponsor'
        ]
        for p in noise_patterns:
            text = re.sub(p, ' ', text, flags=re.I)
        
        # 3. Standardize Nulls
        null_vals = ["NIL", "N/A", "NONE", "NONE YET", "NAN"]
        if text.strip().upper() in null_vals:
            return ""
        
        # 4. Clean Whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _extract_from_trial_page(self):
        """Fallback: extract data from the trial detail HTML page."""
        data = {}
        try:
            rows = self.driver.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 2:
                        label = ""
                        try:
                            bolds = cells[0].find_elements(By.TAG_NAME, "b")
                            if bolds:
                                label = bolds[0].text.strip()
                        except Exception:
                            pass
                        if not label:
                            label = cells[0].text.strip()
                        value = cells[1].text.strip()
                        extras = []
                        for c in cells[2:]:
                            t = c.text.strip()
                            if t:
                                extras.append(t)
                        if label and not label.startswith("Modification") and not label.startswith("Close"):
                            label = re.sub(r'\s+', ' ', label).strip()
                            full = value
                            if extras:
                                full += " | " + " | ".join(extras)
                            if full:
                                data[label] = full
                except (StaleElementReferenceException, Exception):
                    continue
            if "CTRI Number" not in data:
                try:
                    txt = self.driver.find_element(By.TAG_NAME, "body").text
                    m = re.search(r'(CTRI/\d{4}/\d{2,3}/\d+)', txt)
                    if m:
                        data["CTRI Number"] = m.group(1)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Fallback extraction error: {e}")
        return data

    # ------------------------------------------------------------------
    # SINGLE TRIAL
    # ------------------------------------------------------------------
    def _recover_session(self):
        """Navigate to CTRI home page to re-establish a valid session after popup hijack."""
        try:
            logger.info("    🔄 Re-establishing session via CTRI homepage...")
            self._close_extra_tabs()
            self.driver.get(self.CTRI_URL)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            self._dismiss_popups()
            time.sleep(2)
            # Sync fresh cookies
            self._sync_cookies_to_requests()
            logger.info("    ✓ Session recovered via homepage")
            return True
        except Exception as e:
            logger.warning(f"    Session recovery failed: {str(e)[:80]}")
            return False

    def _scrape_one_trial(self, trial_url, trial_num, total):
        # --- FIX: Ensure strict single-tab mode before starting trial ---
        # This prevents "Celebration" popups from previous trials from hijacking the session
        self._close_extra_tabs()
        
        prefix = f"[{trial_num}/{total}]"

        for attempt in range(1, 4):
            try:
                if self.interrupted:
                    return None
                if not self._driver_alive():
                    if not self._recover():
                        return None

                # Step 1: Open trial detail page
                logger.info(f"{prefix} Opening trial page...")
                if not self._safe_get(trial_url):
                    logger.error(f"{prefix} Could not load trial page")
                    return None

                # Dismiss any celebration/promo popups
                self._dismiss_popups()
                self._wait_for_page(timeout=20)

                # Verify we're actually on the trial page, not a popup or login
                current_url = self.driver.current_url.lower()
                if "pmaindet2.php" not in current_url:
                    if "login.php" in current_url:
                        logger.warning(f"{prefix} ⚠ Landed on login page — recovering session...")
                        # KEY FIX: Navigate to CTRI home first to re-establish session
                        self._recover_session()
                    else:
                        logger.warning(f"{prefix} ⚠ Not on trial page ({current_url[:60]}), retrying...")
                    self._dismiss_popups()
                    if not self._safe_get(trial_url):
                        return None
                    self._dismiss_popups()
                    self._wait_for_page(timeout=20)

                    # Check AGAIN — if still not on trial page, try one more recovery
                    current_url = self.driver.current_url.lower()
                    if "pmaindet2.php" not in current_url:
                        logger.warning(f"{prefix} ⚠ Still not on trial page after recovery, final attempt...")
                        self._recover_session()
                        if not self._safe_get(trial_url):
                            return None
                        self._dismiss_popups()
                        self._wait_for_page(timeout=20)

                # Re-sync cookies NOW that we're on the trial page
                # This ensures PDF download gets fresh, valid session cookies
                self._sync_cookies_to_requests()

                # Extract CTRI number for logging
                ctri_id = ""
                try:
                    txt = self.driver.find_element(By.TAG_NAME, "body").text
                    m = re.search(r'(CTRI/\d{4}/\d{2,3}/\d+)', txt)
                    if m:
                        ctri_id = m.group(1)
                except Exception:
                    pass

                logger.info(f"{prefix} Trial loaded: {ctri_id or '(ID pending)'}")

                # Step 2: Download & parse PDF via requests
                pdf_data = self._download_and_parse_pdf()

                if pdf_data and len(pdf_data) >= 3:
                    field_count = len([v for v in pdf_data.values() if v])
                    logger.info(f"{prefix} ✅ PDF: {field_count} fields extracted")
                else:
                    # PDF failed — maybe session lost due to popup, retry once
                    logger.warning(f"{prefix} PDF got {len(pdf_data)} fields, recovering session & retrying PDF...")
                    # Full session recovery: homepage → trial page → sync cookies → PDF
                    self._dismiss_popups()
                    self._recover_session()
                    self._safe_get(trial_url)
                    self._dismiss_popups()
                    self._wait_for_page(timeout=15)
                    self._sync_cookies_to_requests()
                    pdf_data = self._download_and_parse_pdf()

                    if pdf_data and len(pdf_data) >= 3:
                        field_count = len([v for v in pdf_data.values() if v])
                        logger.info(f"{prefix} ✅ PDF retry: {field_count} fields")
                    else:
                        # Final fallback to HTML scraping
                        logger.info(f"{prefix} PDF retry got {len(pdf_data)} fields, HTML fallback...")
                        fallback = self._extract_from_trial_page()
                        if fallback:
                            fallback.update(pdf_data)
                            pdf_data = fallback
                            logger.info(f"{prefix} 📋 Fallback: {len(pdf_data)} fields total")

                if pdf_data:
                    pdf_data["Source_URL"] = trial_url
                    pdf_data["Scraped_At"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if ctri_id and "CTRI_Number" not in pdf_data:
                        pdf_data["CTRI_Number"] = ctri_id
                    return pdf_data
                else:
                    logger.error(f"{prefix} ❌ No data extracted at all")
                    return None

            except WebDriverException as e:
                logger.error(f"{prefix} WebDriver crash (attempt {attempt}/3): {str(e)[:100]}")
                if attempt < 3:
                    self._recover()
                    time.sleep(1)
            except Exception as e:
                logger.error(f"{prefix} Error (attempt {attempt}/3): {str(e)[:120]}")
                if attempt < 3:
                    time.sleep(1)

        return None

    # ------------------------------------------------------------------
    # CATEGORY
    # ------------------------------------------------------------------
    def _scrape_batch(self, batch, start_num, total, cat_name):
        """Scrape a batch of trials using separate WebDriver instances."""
        results = []
        
        def worker(trial_info):
            url, trial_num = trial_info
            prefix = f"[{trial_num}/{total}]"
            
            # Step 1: Claim a driver from the pool
            driver = self.driver_pool.get()
            self._thread_local.driver = driver
            self._thread_local.wait = None  # Force wait property to re-create for this driver
            
            # Ensure fresh session per thread
            if not hasattr(self._thread_local, "session"):
                self._thread_local.session = requests.Session()
            
            try:
                # Use individual driver for this trial
                data = self._scrape_one_trial(url, trial_num, total)
                return (url, trial_num, data)
            except Exception as e:
                logger.error(f"{prefix} Worker failed: {e}")
                return (url, trial_num, None)
            finally:
                # Release driver back to pool
                self.driver_pool.put(driver)

        # Process batch in parallel using the driver pool
        with ThreadPoolExecutor(max_workers=NUM_TABS) as executor:
            # Use submit + as_completed to allow checking for interruption
            futures = {executor.submit(worker, item): item for item in batch}
            try:
                for future in as_completed(futures):
                    if self.interrupted:
                        break
                    results.append(future.result())
            except KeyboardInterrupt:
                self.interrupted = True
                logger.warning("⚠️ Interrupt detected in batch processing")
                raise
            except Exception as e:
                logger.error(f"Batch execution error: {e}")

        return results

    def _scrape_category(self, cat_name, cat_value, start_from=0):
        banner(f"CATEGORY: {cat_name}")

        if not self._search_category(cat_name, cat_value):
            logger.error(f"Search failed: {cat_name}")
            return

        trial_urls = self._get_trial_urls()
        if not trial_urls:
            logger.warning(f"No trials found: {cat_name}")
            return

        total = len(trial_urls)
        logger.info(f"📊 {total} trials (starting from #{start_from}), using {NUM_TABS}-tab batches")

        scraped_count = 0

        # Build list of (url, trial_num) pairs, skipping already-done
        work_items = [(url, i + 1) for i, url in enumerate(trial_urls) if i >= start_from]

        # Process in batches of NUM_TABS
        for batch_start in range(0, len(work_items), NUM_TABS):
            batch = work_items[batch_start:batch_start + NUM_TABS]
            batch_nums = [t[1] for t in batch]
            logger.info(f"\n{'='*50}\n  BATCH: Trials {batch_nums}\n{'='*50}")

            try:
                batch_results = self._scrape_batch(batch, batch_start, total, cat_name)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.error(f"Batch error: {e}")
                # Fallback: scrape each trial individually
                batch_results = []
                for url, trial_num in batch:
                    data = self._scrape_one_trial(url, trial_num, total)
                    batch_results.append((url, trial_num, data))

            for url, trial_num, data in batch_results:
                if data:
                    self.scraped_counter += 1
                    data["Scraped_ID"] = f"scraped_trial_{self.scraped_counter}"
                    data["Search_Category"] = cat_name
                    self.all_trials.append(data)
                    scraped_count += 1
                    self.stats["scraped"] += 1
                    ctri_id = data.get("CTRI_Number", "")
                    filled = len([v for v in data.values() if v])
                    logger.info(f"[{trial_num}/{total}] ✅ {ctri_id or 'no-ID'} ({filled} fields) [ID: scraped_trial_{self.scraped_counter}]")
                else:
                    self.failed_urls.append(url)
                    self.stats["failed"] += 1
                    logger.error(f"[{trial_num}/{total}] ❌ FAILED")

            # Save progress after each batch
            self._save_progress()
            time.sleep(self.between_trials)

        self._save_progress()
        banner(f"CATEGORY DONE: {cat_name}")
        print(f"  Scraped: {scraped_count}  |  Total DB: {len(self.all_trials)}")

    # ------------------------------------------------------------------
    # MAIN
    # ------------------------------------------------------------------
    def run(self, resume_category=0, resume_trial=0):
        try:
            if not self._init_pool():
                return

            banner("CTRI CANCER TRIALS SCRAPER", "=")
            print(f"  Categories:  {len(self.SEARCH_CATEGORIES)}")
            print(f"  Resume from: Category #{resume_category}, Trial #{resume_trial}")
            print(f"  Concurrency: {len(self.drivers_list)} Brave instances")
            if not self.all_trials:
                print(f"  Fresh start: YES (deleting old CSVs)")
            else:
                print(f"  Resuming: YES (keeping {len(self.all_trials)} existing trials)")

            # Cleanup old data if fresh start and no existing data loaded
            if resume_category == 0 and resume_trial == 0 and not self.all_trials:
                for f in ["ctri_final.log", CSV_PROGRESS, CSV_FINAL]:
                    if os.path.exists(f):
                        try:
                            # Try to close any and all file handles by suggesting to user or attempting delete
                            os.remove(f)
                            logger.info(f"🗑 Deleted old file: {f}")
                        except Exception as e:
                            logger.warning(f"Could not delete {f}: {e}")
                # Re-initialize logging to fresh file
                for handler in logger.handlers[:]:
                    logger.removeHandler(handler)
                logging.basicConfig(
                    level=logging.INFO,
                    format="%(asctime)s | %(levelname)-7s | %(message)s",
                    datefmt="%H:%M:%S",
                    handlers=[
                        logging.FileHandler("ctri_final.log", encoding="utf-8", mode="w"),
                        logging.StreamHandler(sys.stdout),
                    ],
                )

            proceed = input("\n  Start? (yes/no): ").strip().lower()
            if proceed not in ("yes", "y"):
                print("Aborted.")
                return

            # Use the first driver to perform search and get trial URLs
            self._thread_local.driver = self.drivers_list[0]

            for idx, (cat_name, cat_value) in enumerate(self.SEARCH_CATEGORIES):
                if idx < resume_category:
                    logger.info(f"⏭ Skipping: {cat_name}")
                    continue

                if cat_name == "Name of Principle Investigator":
                     logger.info("skipping this because has no trials \" # (\"Name of Principle Investigator\", \"3\"), not going to this category because has 0 trials\"")
                     continue
                start = resume_trial if idx == resume_category else 0
                try:
                    self._scrape_category(cat_name, cat_value, start_from=start)
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    logger.error(f"Category error ({cat_name}): {e}")
                    continue

            self._save_progress()
            self._save_final()

            banner("SCRAPING COMPLETE", "=")
            print(f"  Total:      {len(self.all_trials)}")
            print(f"  Scraped:    {self.stats['scraped']}")
            print(f"  Failed:     {self.stats['failed']}")
            if self.all_trials:
                print(f"  Columns:    {len(FIXED_COLUMNS)} (fixed schema)")
            print(f"\n  Output: {CSV_FINAL}")

        except KeyboardInterrupt:
            self.interrupted = True
            banner("INTERRUPTED", "=")
            self._save_progress()
            self._save_final()
            print(f"  Saved {len(self.all_trials)} trials")

        finally:
            self.interrupted = True
            self._close_pool()


# ==============================================================================
# ENTRY
# ==============================================================================
def main():
    banner("CTRI CANCER CLINICAL TRIALS SCRAPER", "=")
    print("  Flow: Search → Captcha → View → Download PDF → Extract ALL data")
    print("  Uses: pdfplumber (downloads PDF, no browser PDF viewer issues)")
    print("  Output: ctri_cancer_trials_FINAL.csv (FRESH — no old data)")
    print()

    scraper = CTRIScraper()
    scraper.run(resume_category=0, resume_trial=0)  # RESTART


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted")
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
