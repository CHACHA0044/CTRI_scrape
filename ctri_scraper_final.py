#!/usr/bin/env python3
"""
CTRI Cancer Clinical Trials Scraper - PRODUCTION VERSION
Features: Auto-recovery, crash resistance, resume capability, ultra-fast
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import logging
import re
from datetime import datetime
import sys
import os

# Configure logging with UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ctri_final.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class CTRIFinalScraper:
    def __init__(self):
        self.driver = None
        self.wait_time = 5
        self.all_trials_data = []
        self.failed_urls = []
        self.current_category_index = 0
        self.current_trial_index = 0
        
        # Load existing progress
        self.load_progress()
        
        # Search categories
        self.search_categories = [
            ("Public title of study", "11"),
            ("Scientific title of study", "2"),
            ("Health Condition/ Problem Studied", "3"),
            ("Name of Principle Investigator", "4"),
            ("Primary outcome/s", "6"),
            ("Secondary outcome/s", "7"),
            ("Site/s of study", "8"),
            ("Ethics Committee", "9"),
            ("Primary Sponsor", "10"),
        ]
        
    def load_progress(self):
        """Load existing progress to avoid re-scraping"""
        progress_file = "ctri_cancer_trials_PROGRESS.csv"
        if os.path.exists(progress_file):
            try:
                df = pd.read_csv(progress_file, encoding='utf-8-sig')
                self.all_trials_data = df.to_dict('records')
                logger.info(f"[LOADED] {len(self.all_trials_data)} existing trials")
            except Exception as e:
                logger.warning(f"Could not load progress: {e}")
                
    def setup_driver(self):
        """Initialize Chrome WebDriver with crash protection"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
        
        logger.info("Setting up Chrome WebDriver...")
        
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Speed optimizations
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.page_load_strategy = 'eager'
        
        # Prevent crashes
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-logging")
        
        try:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=chrome_options
            )
            self.wait = WebDriverWait(self.driver, self.wait_time)
            logger.info("WebDriver ready")
            return True
        except Exception as e:
            logger.error(f"Failed to setup driver: {e}")
            return False
            
    def is_driver_alive(self):
        """Check if driver is still functional"""
        try:
            _ = self.driver.current_url
            return True
        except:
            return False
            
    def recover_from_crash(self):
        """Recover from browser crash"""
        logger.warning("Recovering from crash...")
        time.sleep(2)
        
        if self.setup_driver():
            logger.info("Recovery successful")
            return True
        else:
            logger.error("Recovery failed")
            return False
            
    def search_category(self, category_name, category_value, keyword="cancer"):
        """Perform search with CAPTCHA"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Check if driver is alive
                if not self.is_driver_alive():
                    if not self.recover_from_crash():
                        return False
                
                logger.info(f"Searching '{category_name}' (Attempt {attempt + 1})")
                
                self.driver.get("https://ctri.nic.in/Clinicaltrials/pubview.php")
                time.sleep(1.5)
                
                keyword_input = self.wait.until(
                    EC.presence_of_element_located((By.NAME, "searchword"))
                )
                keyword_input.clear()
                keyword_input.send_keys(keyword)
                
                select_element = Select(self.driver.find_element(By.NAME, "searchtype"))
                select_element.select_by_value(category_value)
                
                print("\n" + "="*70)
                print(f"CAPTCHA - {category_name}")
                print("="*70)
                print("1. Enter CAPTCHA")
                print("2. Click Search")
                print("3. Wait for results")
                print("="*70)
                input("\nPress ENTER after search...")
                
                time.sleep(1)
                
                if "pubview2.php" in self.driver.current_url:
                    logger.info("Search successful")
                    return True
                    
            except Exception as e:
                logger.error(f"Search error: {e}")
                time.sleep(2)
                
        return False
    
    def get_trial_urls(self):
        """Extract trial URLs from search results"""
        logger.info("Extracting URLs...")
        trial_urls = []
        
        try:
            time.sleep(1.5)
            
            page_source = self.driver.page_source
            matches = re.findall(r"newwin2\('([^']+)'\)", page_source)
            
            for match in matches:
                if "pmaindet2.php" in match:
                    full_url = f"https://ctri.nic.in/Clinicaltrials/{match}"
                    if full_url not in trial_urls:
                        trial_urls.append(full_url)
            
            logger.info(f"Found {len(trial_urls)} URLs")
            
        except Exception as e:
            logger.error(f"URL extraction error: {e}")
        
        return trial_urls
    
    def extract_field(self, label_text):
        """Extract field value"""
        try:
            xpath = f"//td/b[contains(text(), '{label_text}')]/parent::td/following-sibling::td[1]"
            elements = self.driver.find_elements(By.XPATH, xpath)
            if elements and elements[0].text.strip():
                return elements[0].text.strip()
        except:
            pass
        return ""
    
    def scrape_trial(self, url):
        """Scrape single trial with crash recovery"""
        max_retries = 3
        
        for retry in range(max_retries):
            try:
                # Check driver health
                if not self.is_driver_alive():
                    logger.warning("Driver dead, recovering...")
                    if not self.recover_from_crash():
                        return None
                
                logger.info(f"Scraping: {url}")
                
                self.driver.get(url)
                time.sleep(1)
                
                self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                
                # Extract data
                data = {
                    'CTRI Number': self.extract_field('CTRI Number'),
                    'Study Title': self.extract_field('Public Title'),
                    'Study URL': url,
                    'Acronym': self.extract_field('Acronym'),
                    'Study Status': self.extract_field('Recruitment Status'),
                    'Brief Summary': self.extract_field('Brief Summary'),
                    'Study Results': self.extract_field('Publication of Results'),
                    'Conditions': self.extract_field('Health Condition'),
                    'Interventions': self.extract_field('Intervention'),
                    'Primary Outcome Measures': self.extract_field('Primary Outcome'),
                    'Secondary Outcome Measures': self.extract_field('Secondary Outcome'),
                    'Other Outcome Measures': self.extract_field('Other Outcome'),
                    'Sponsor': self.extract_field('Primary Sponsor'),
                    'Collaborators': self.extract_field('Secondary Sponsor'),
                    'Sex': self.extract_field('Gender'),
                    'Age': self.extract_field('Age'),
                    'Phases': self.extract_field('Phase'),
                    'Enrollment': self.extract_field('Sample Size'),
                    'Funder Type': self.extract_field('Source of Monetary'),
                    'Study Type': self.extract_field('Type of Trial'),
                    'Study Design': self.extract_field('Study Design'),
                    'Other IDs': self.extract_field('Secondary ID'),
                    'Start Date': self.extract_field('Date of First Enrollment'),
                    'Primary Completion Date': self.extract_field('Primary Completion'),
                    'Completion Date': self.extract_field('Study Completion'),
                    'First Posted': self.extract_field('Date of Registration'),
                    'Results First Posted': self.extract_field('Results First Posted'),
                    'Last Update Posted': self.extract_field('Last Modified'),
                    'Locations': self.extract_field('Site'),
                    'Study Documents': self.extract_field('Study Documents'),
                }
                
                # Get CTRI from page if missing
                if not data['CTRI Number']:
                    page_text = self.driver.find_element(By.TAG_NAME, "body").text
                    match = re.search(r'CTRI/\d{4}/\d{2}/\d+', page_text)
                    if match:
                        data['CTRI Number'] = match.group(0)
                
                ctri = data.get('CTRI Number', 'UNKNOWN')
                logger.info(f"[OK] {ctri}")
                
                return data
                
            except WebDriverException as e:
                logger.error(f"WebDriver error (retry {retry+1}/{max_retries}): {str(e)[:100]}")
                if retry < max_retries - 1:
                    if not self.recover_from_crash():
                        return None
                    time.sleep(2)
                else:
                    return None
                    
            except Exception as e:
                logger.error(f"Scraping error: {e}")
                return None
        
        return None
    
    def scrape_category(self, cat_name, cat_value, start_from=0):
        """Scrape all trials from category"""
        logger.info(f"\n{'='*70}")
        logger.info(f"CATEGORY: {cat_name}")
        logger.info(f"{'='*70}")
        
        # Search
        if not self.search_category(cat_name, cat_value):
            logger.error(f"Search failed for {cat_name}")
            return False
        
        # Get URLs
        trial_urls = self.get_trial_urls()
        
        if not trial_urls:
            logger.warning(f"No trials found for {cat_name}")
            return False
        
        logger.info(f"Processing {len(trial_urls)} trials (starting from {start_from})...")
        
        # Get existing CTRI numbers to avoid duplicates
        existing_ctri = {t.get('CTRI Number') for t in self.all_trials_data if t.get('CTRI Number')}
        
        # Scrape each trial
        for i, url in enumerate(trial_urls, 1):
            # Skip if before start point
            if i < start_from:
                continue
            
            print(f"\n[{cat_name}] Trial {i}/{len(trial_urls)}")
            
            data = self.scrape_trial(url)
            
            if data and data.get('CTRI Number'):
                ctri = data['CTRI Number']
                
                if ctri not in existing_ctri:
                    self.all_trials_data.append(data)
                    existing_ctri.add(ctri)
                    print(f"[NEW] {ctri}")
                else:
                    print(f"[DUP] {ctri}")
            else:
                print(f"[FAIL] Could not extract data")
                self.failed_urls.append(url)
            
            time.sleep(0.8)  # Faster
            
            # Save every 25 trials
            if i % 25 == 0:
                self.save_progress()
        
        print(f"\n{'='*70}")
        print(f"Total unique trials: {len(self.all_trials_data)}")
        print(f"{'='*70}\n")
        
        return True
    
    def save_progress(self):
        """Save progress"""
        if self.all_trials_data:
            try:
                df = pd.DataFrame(self.all_trials_data)
                df.to_csv("ctri_cancer_trials_PROGRESS.csv", index=False, encoding='utf-8-sig')
                logger.info(f"[SAVED] {len(df)} trials")
            except Exception as e:
                logger.error(f"Save error: {e}")
    
    def run(self, resume_category=0, resume_trial=1315):
        """Main execution with resume capability"""
        try:
            if not self.setup_driver():
                logger.error("Failed to setup driver")
                return None
            
            print("\n" + "="*70)
            print("CTRI SCRAPER - PRODUCTION VERSION")
            print("="*70)
            print(f"Total categories: {len(self.search_categories)}")
            print(f"Resume: Category {resume_category}, Trial {resume_trial}")
            print(f"Existing trials: {len(self.all_trials_data)}")
            print("="*70)
            
            for idx, (cat_name, cat_value) in enumerate(self.search_categories):
                # Skip categories before resume point
                if idx < resume_category:
                    logger.info(f"Skipping category {idx}: {cat_name}")
                    continue
                
                try:
                    # Determine start trial
                    start_trial = resume_trial if idx == resume_category else 1
                    
                    self.scrape_category(cat_name, cat_value, start_from=start_trial)
                    
                except Exception as e:
                    logger.error(f"Category error ({cat_name}): {e}")
                    continue
            
            print("\n" + "="*70)
            print("SCRAPING COMPLETE")
            print(f"Total trials: {len(self.all_trials_data)}")
            print(f"Failed URLs: {len(self.failed_urls)}")
            print("="*70)
            
            if self.all_trials_data:
                return pd.DataFrame(self.all_trials_data)
            return None
            
        except KeyboardInterrupt:
            print("\n[INTERRUPTED]")
            logger.info("User interrupted")
            if self.all_trials_data:
                return pd.DataFrame(self.all_trials_data)
            return None
            
        finally:
            try:
                if self.driver:
                    self.driver.quit()
            except:
                pass
    
    def save_final(self, df):
        """Save final results"""
        if df is None or df.empty:
            print("No data to save")
            return
        
        csv_file = "ctri_cancer_trials_FINAL.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        
        print(f"\n[SUCCESS] Saved {len(df)} trials to {csv_file}")
        
        return csv_file


def main():
    print("\n" + "="*70)
    print("CTRI SCRAPER - PRODUCTION VERSION")
    print("="*70)
    print("\nFeatures:")
    print("  • Auto-recovery from crashes")
    print("  • Resume from trial 1315")
    print("  • Ultra-fast (0.8s per trial)")
    print("  • Single CSV output")
    print("  • Progress auto-save every 25 trials")
    print("\nEstimated time: 15-25 minutes")
    print("="*70)
    
    proceed = input("\nStart? (yes/no): ").strip().lower()
    if proceed not in ['yes', 'y']:
        return
    
    # Resume from trial 1315 in first category
    scraper = CTRIFinalScraper()
    df = scraper.run(resume_category=0, resume_trial=1315)
    
    if df is not None and not df.empty:
        print("\n" + "="*70)
        print("RESULTS")
        print("="*70)
        print(f"Total trials: {len(df)}")
        print(f"Columns: {len(df.columns)}")
        
        print("\nSample:")
        cols = ['CTRI Number', 'Study Title', 'Study Status']
        available = [c for c in cols if c in df.columns]
        if available:
            print(df[available].head(3).to_string())
        
        scraper.save_final(df)
        
        print("\n" + "="*70)
        print("SUCCESS - Check ctri_cancer_trials_FINAL.csv")
        print("="*70)
    else:
        print("\nNo data collected")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
