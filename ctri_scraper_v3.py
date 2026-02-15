#!/usr/bin/env python3
"""
CTRI Cancer Clinical Trials Scraper - COMPLETE REWRITE
Fixes URL extraction and navigation issues
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import logging
import re
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ctri_scraper_v3.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CTRICancerScraperV3:
    def __init__(self):
        self.driver = None
        self.wait_time = 15
        self.all_trials_data = []
        
        # Search categories - correct values from dropdown
        self.search_categories = [
            ("Public title of study", "1"),
            ("Scientific title of study", "2"),
            ("Health Condition/ Problem Studied", "3"),
            ("Name of Principle Investigator", "4"),
            ("Primary outcome/s", "6"),
            ("Secondary outcome/s", "7"),
            ("Site/s of study", "8"),
            ("Ethics Committee", "9"),
            ("Primary Sponsor", "10"),
        ]
        
    def setup_driver(self):
        """Initialize Chrome WebDriver"""
        logger.info("Setting up Chrome WebDriver...")
        
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        
        self.wait = WebDriverWait(self.driver, self.wait_time)
        logger.info("WebDriver ready!")
        
    def search_category(self, category_name, category_value, keyword="cancer"):
        """Perform search with CAPTCHA"""
        max_retries = 2
        
        for attempt in range(max_retries + 1):
            try:
                logger.info(f"Searching '{category_name}' (Attempt {attempt + 1}/{max_retries + 1})")
                
                # Navigate to search page
                self.driver.get("https://ctri.nic.in/Clinicaltrials/pubview.php")
                time.sleep(3)
                
                # Enter keyword
                keyword_input = self.wait.until(
                    EC.presence_of_element_located((By.NAME, "searchword"))
                )
                keyword_input.clear()
                keyword_input.send_keys(keyword)
                
                # Select category
                select_element = Select(self.driver.find_element(By.NAME, "searchtype"))
                select_element.select_by_value(category_value)
                
                # Manual CAPTCHA
                print("\n" + "="*70)
                print(f"CAPTCHA REQUIRED - {category_name}")
                print("="*70)
                print("1. Enter CAPTCHA")
                print("2. Click Search")
                print("3. Wait for results")
                print("="*70)
                input("\nPress ENTER after search completes...")
                
                time.sleep(2)
                
                if "pubview2.php" in self.driver.current_url:
                    logger.info("Search successful!")
                    return True
                    
            except Exception as e:
                logger.error(f"Search error: {e}")
                
        return False
    
    def get_trial_urls_from_results(self):
        """Extract actual trial URLs from search results page"""
        logger.info("Extracting trial URLs from search results...")
        trial_urls = []
        
        try:
            time.sleep(3)
            
            # Strategy 1: Look for links in table cells with CTRI numbers
            # The CTRI numbers are in the first column of the results table
            ctri_links = self.driver.find_elements(By.XPATH, "//table//tr//td[1]//a")
            
            logger.info(f"Found {len(ctri_links)} links in first table column")
            
            for link in ctri_links:
                try:
                    # Get the href attribute
                    href = link.get_attribute("href")
                    onclick = link.get_attribute("onclick")
                    link_text = link.text.strip()
                    
                    # Debug first few
                    if len(trial_urls) < 3:
                        logger.info(f"Sample link - Text: {link_text}, Href: {href}, Onclick: {onclick}")
                    
                    # Try onclick first
                    if onclick:
                        # Look for the pmaindet2.php pattern
                        match = re.search(r"newwin2\('([^']+)'\)", onclick)
                        if match:
                            relative_path = match.group(1)
                            full_url = f"https://ctri.nic.in/Clinicaltrials/{relative_path}"
                            if full_url not in trial_urls:
                                trial_urls.append(full_url)
                                continue
                    
                    # Try href 
                    if href and "pmaindet2.php" in href:
                        if href not in trial_urls:
                            trial_urls.append(href)
                            continue
                    
                    # If the link has a CTRI number, we might need to construct URL manually
                    if link_text and link_text.startswith("CTRI/"):
                        # This CTRI number exists but we can't extract the URL
                        # We'll need to click it or find another way
                        logger.debug(f"Found CTRI {link_text} but couldn't extract URL")
                        
                except Exception as e:
                    logger.debug(f"Error processing link: {e}")
                    continue
            
            # Strategy 2: If no URLs found, try all links with "View" text
            if not trial_urls:
                logger.info("Trying alternate strategy: looking for 'View' links...")
                view_links = self.driver.find_elements(By.XPATH, "//a[contains(text(), 'View')]")
                logger.info(f"Found {len(view_links)} 'View' links")
                
                for link in view_links:
                    try:
                        href = link.get_attribute("href")
                        onclick = link.get_attribute("onclick")
                        
                        # The href might be "javascript:newwin2('pmaindet2.php?...')"
                        # We need to extract the actual path from it
                        if href and "javascript:" in href:
                            # Extract from: javascript:newwin2('pmaindet2.php?EncHid=...')
                            match = re.search(r"newwin2\('([^']+)'\)", href)
                            if match:
                                relative_path = match.group(1)
                                full_url = f"https://ctri.nic.in/Clinicaltrials/{relative_path}"
                                if full_url not in trial_urls:
                                    trial_urls.append(full_url)
                                    continue
                        
                        # Try onclick attribute
                        if onclick:
                            match = re.search(r"newwin2\('([^']+)'\)", onclick)
                            if match:
                                relative_path = match.group(1)
                                full_url = f"https://ctri.nic.in/Clinicaltrials/{relative_path}"
                                if full_url not in trial_urls:
                                    trial_urls.append(full_url)
                                    continue
                        
                        # Try regular href
                        if href and "pmaindet2.php" in href and "javascript" not in href:
                            if href not in trial_urls:
                                trial_urls.append(href)
                    except:
                        continue
            
            # Strategy 3: If still no URLs, extract from page source directly
            if not trial_urls:
                logger.info("Trying alternate strategy: parsing page source...")
                page_source = self.driver.page_source
                
                # Find all newwin2 calls in the page source
                matches = re.findall(r"newwin2\('([^']+)'\)", page_source)
                logger.info(f"Found {len(matches)} newwin2 calls in page source")
                
                for match in matches:
                    if "pmaindet2.php" in match:
                        full_url = f"https://ctri.nic.in/Clinicaltrials/{match}"
                        if full_url not in trial_urls:
                            trial_urls.append(full_url)
            
            logger.info(f"Extracted {len(trial_urls)} trial URLs")
            
            # Debug: Print first 3 URLs
            if trial_urls:
                logger.info("Sample URLs:")
                for url in trial_urls[:3]:
                    logger.info(f"  {url}")
            else:
                logger.warning("No trial URLs found - debugging...")
                logger.info(f"Current page URL: {self.driver.current_url}")
                
                # Save page source for debugging
                try:
                    with open("debug_page_source.html", "w", encoding="utf-8") as f:
                        f.write(self.driver.page_source)
                    logger.info("Saved page source to debug_page_source.html")
                except:
                    pass
            
        except Exception as e:
            logger.error(f"Error extracting URLs: {e}")
        
        return trial_urls
    
    def extract_field(self, label_text):
        """Extract field value by label text"""
        try:
            # Try to find table cell with this label
            xpaths = [
                f"//td/b[contains(text(), '{label_text}')]/parent::td/following-sibling::td[1]",
                f"//th[contains(text(), '{label_text}')]/following-sibling::td[1]",
                f"//b[contains(text(), '{label_text}')]/parent::*/following-sibling::*[1]",
            ]
            
            for xpath in xpaths:
                try:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    if elements and elements[0].text.strip():
                        return elements[0].text.strip()
                except:
                    continue
                    
        except:
            pass
        
        return ""
    
    def scrape_trial_details(self, trial_url):
        """Scrape all details from a single trial page"""
        logger.info(f"Scraping: {trial_url}")
        
        try:
            # Navigate to the trial page
            self.driver.get(trial_url)
            time.sleep(3)
            
            # Wait for page to load
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Extract all required fields
            data = {
                'Study URL': trial_url,
                'CTRI Number': self.extract_field('CTRI Number'),
                'Study Title': self.extract_field('Public Title'),
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
            
            # Try to get CTRI number from page if not found
            if not data['CTRI Number']:
                page_text = self.driver.find_element(By.TAG_NAME, "body").text
                match = re.search(r'CTRI/\d{4}/\d{2}/\d+', page_text)
                if match:
                    data['CTRI Number'] = match.group(0)
            
            ctri = data.get('CTRI Number', 'UNKNOWN')
            logger.info(f"✓ Extracted: {ctri}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error scraping trial: {e}")
            return None
    
    def scrape_category_trials(self, category_name, category_value):
        """Scrape all trials from one category"""
        logger.info(f"\n{'='*70}")
        logger.info(f"CATEGORY: {category_name}")
        logger.info(f"{'='*70}")
        
        # Search
        if not self.search_category(category_name, category_value):
            logger.error(f"Search failed for {category_name}")
            return
        
        # Get URLs
        trial_urls = self.get_trial_urls_from_results()
        
        if not trial_urls:
            logger.warning(f"No trials found for {category_name}")
            return
        
        logger.info(f"Processing {len(trial_urls)} trials...")
        
        # Scrape each trial
        for i, url in enumerate(trial_urls, 1):
            print(f"\n[{category_name}] Trial {i}/{len(trial_urls)}")
            
            data = self.scrape_trial_details(url)
            
            if data and data.get('CTRI Number'):
                ctri = data['CTRI Number']
                
                # Check for duplicates
                existing = [t for t in self.all_trials_data if t.get('CTRI Number') == ctri]
                
                if not existing:
                    self.all_trials_data.append(data)
                    print(f"[NEW] {ctri}")
                else:
                    print(f"[DUP] {ctri}")
            else:
                print(f"[FAIL] Could not extract data")
            
            time.sleep(2)
            
            # Save progress every 50 trials
            if i % 50 == 0:
                self.save_progress()
        
        print(f"\n{'='*70}")
        print(f"Total unique trials so far: {len(self.all_trials_data)}")
        print(f"{'='*70}\n")
    
    def save_progress(self):
        """Save progress"""
        if self.all_trials_data:
            try:
                df = pd.DataFrame(self.all_trials_data)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"ctri_progress_{timestamp}.csv"
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                logger.info(f"Progress saved: {filename}")
            except Exception as e:
                logger.error(f"Save error: {e}")
    
    def run(self):
        """Main execution"""
        try:
            self.setup_driver()
            
            print("\n" + "="*70)
            print("CTRI SCRAPER V3 - COMPLETE REWRITE")
            print("="*70)
            print(f"Categories to search: {len(self.search_categories)}")
            print("="*70)
            
            for cat_name, cat_value in self.search_categories:
                try:
                    self.scrape_category_trials(cat_name, cat_value)
                except Exception as e:
                    logger.error(f"Error with category {cat_name}: {e}")
                    continue
            
            print("\n" + "="*70)
            print("SCRAPING COMPLETE!")
            print(f"Total trials: {len(self.all_trials_data)}")
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
            input("\nPress ENTER to close browser...")
            if self.driver:
                self.driver.quit()
    
    def save_results(self, df):
        """Save final results"""
        if df is None or df.empty:
            print("No data to save")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_file = f"ctri_cancer_trials_{timestamp}.csv"
        excel_file = csv_file.replace('.csv', '.xlsx')
        
        # Save CSV
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"\n✓ CSV saved: {csv_file}")
        
        # Save Excel
        try:
            df.to_excel(excel_file, index=False, engine='openpyxl')
            print(f"✓ Excel saved: {excel_file}")
        except:
            print("✗ Excel save failed")
        
        return csv_file


def main():
    print("\n" + "="*70)
    print("CTRI CANCER TRIALS SCRAPER V3")
    print("="*70)
    print("\nFeatures:")
    print("  • Proper URL extraction from JavaScript links")
    print("  • Direct navigation to trial pages")
    print("  • All 28 fields extracted")
    print("  • Auto-save progress every 50 trials")
    print("  • Duplicate removal")
    print("\nEstimated time: 30-60 minutes")
    print("="*70)
    
    proceed = input("\nStart? (yes/no): ").strip().lower()
    if proceed not in ['yes', 'y']:
        return
    
    scraper = CTRICancerScraperV3()
    df = scraper.run()
    
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
        
        scraper.save_results(df)
        
        print("\n" + "="*70)
        print("SUCCESS!")
        print("="*70)
    else:
        print("\nNo data collected - check logs")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
