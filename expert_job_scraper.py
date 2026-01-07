"""
REFACTORED Job Scraper - DOM Analysis Expert Implementation
Follows strict rules to avoid apply pages and extract from job detail pages only
"""
import asyncio
import json
import re
import time
from datetime import datetime
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urlparse, urlencode

import requests
import sys
# Fix Windows console encoding
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except: pass

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from scraper_filters import is_valid_job, matches_target_role, sanitize_url


class ExpertJobScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.seen_urls = set()
        self.visited_gateways = set()
        self.rejected_urls = set()
        # 1. ADD GLOBAL STATE (Safety Guards)
        self.visited_urls = set()
        self.visited_job_urls = set()
        self.used_selenium_for_url = {}


    def should_skip_url(self, url: str) -> bool:
        """2. URL BLOCKLIST (HARD STOP)"""
        # 1. Sanitize first
        url = sanitize_url(url)
        if not url: return True
        
        skipped_terms = [
            'login', 'sign-in', 'my-profile', 'benefits',
            'skip-to-main', 'candidateexperience',
            'auth', 'sso', 'oraclecloud.com',
            'privacy', 'terms', 'cookie'
        ]
        return any(term in url.lower() for term in skipped_terms)
    
    async def extract_jobs_from_site(self, site_url: str, country: str) -> List[Dict]:
        """Extract jobs using expert DOM analysis with recursive gateway handling"""
        self.seen_urls = set() # Reset for new site
        # Reset Safety Guards
        self.visited_urls = set()
        self.visited_job_urls = set()
        self.used_selenium_for_url = {}
        
        try:
            # Use default AsyncWebCrawler (it already supports JS)
            async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
                # RULE #1: Check if URL is apply page - redirect to parent
                clean_url = self._get_job_detail_url(site_url)
                
                # 2. URL BLOCKLIST CHECK
                if self.should_skip_url(clean_url):
                    print(f"  Skipping blocked URL: {clean_url}")
                    return []
                
                print(f"Starting extraction for: {clean_url}")
                
                # Detect if this is a SPA/Workday page
                is_spa = self._is_spa_url(clean_url)
                
                # Step 1: Initial load with JS rendering if SPA
                if is_spa:
                    scan_result = await crawler.arun(
                        url=clean_url,
                        word_count_threshold=10,
                        js_code=["const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms)); await delay(3000);"],
                        wait_for="css:body",
                        delay_before_return_html=2.0
                    )
                else:
                    scan_result = await crawler.arun(url=clean_url, word_count_threshold=10)
                
                scan_soup = None
                if scan_result.success:
                    scan_soup = BeautifulSoup(scan_result.html, 'html.parser')
                    
                    # Check quality of extraction
                    text_content = scan_soup.get_text(strip=True)
                    word_count = len(text_content.split())
                    
                    # Enhanced Retry Logic
                    # If content is too short OR contains specific JS warning text
                    if word_count < 100 or len(text_content) < 500 or "enable javascript" in text_content.lower() or "please enable" in text_content.lower():
                        print(f"  [Crawl4AI] Low quality content detected (Words={word_count}, Len={len(text_content)}). Retrying with Selenium...")
                        selenium_html = self._extract_with_selenium(clean_url)
                        if selenium_html:
                            scan_soup = BeautifulSoup(selenium_html, 'html.parser')
                        else:
                            print("  [Crawl4AI] Selenium retry failed or returned empty.")

                if not scan_soup:
                    print(f"  [Error] Failed to load site: {clean_url}")
                    # Fallback to Selenium if scan_soup is still None
                    print("  [Fallback] Attempting Selenium as last resort...")
                    selenium_html = self._extract_with_selenium(clean_url)
                    if selenium_html:
                        scan_soup = BeautifulSoup(selenium_html, 'html.parser')
                    else:
                        print("  [Fallback] Selenium also failed.")
                        return []
                    
                # STEP 3: Auto-fill Filters (Human-Like Behavior)
                # Check if we can search for the country directly
                search_url = self._detect_and_fill_filters(scan_soup, clean_url, country)
                
                jobs = []
                if search_url:
                    print(f"  Detected Search Filter. Simulating Human Search for '{country}'...")
                    print(f"  Redirecting to: {search_url}")
                    # Try the filtered URL first
                    jobs = await self._recursive_crawl(search_url, country, crawler, depth=0)
                    
                    # ROLLBACK MECHANISM
                    # 6. ZERO-JOBS CHECK FIX
                    if len(jobs) == 0:
                        print(f"  Filter returned 0 jobs. Rolling back to original URL: {clean_url}")
                        # If filter failed (e.g. no results found), try the original page
                        jobs = await self._recursive_crawl(clean_url, country, crawler, depth=0)
                    else:
                        return jobs # TERMINAL SUCCESS
                else:
                    # No filter found, just crawl normally
                    jobs = await self._recursive_crawl(clean_url, country, crawler, depth=0)

                return jobs

        except Exception as e:
            print(f"  [Crawl4AI] Critical Error/Crash: {e}")
            print("  [Fallback] Switching to Selenium due to Crawl4AI crash...")
            
            # EMERGENCY SELENIUM FALLBACK
            # If crawl4ai failed completely, we do a manual Selenium extraction of the ONE page
            clean_url = self._get_job_detail_url(site_url)
            selenium_html = self._extract_with_selenium(clean_url)
            
            fallback_jobs = []
            if selenium_html:
                soup = BeautifulSoup(selenium_html, 'html.parser')
                
                # Check for jobs
                job_cards = self._find_job_cards(soup)
                if len(job_cards) > 0:
                    print(f"  [Selenium-Only] Found {len(job_cards)} job cards. Extracting available info...")
                    
                    for card in job_cards:
                        try:
                            title = card.get_text().strip().split('\n')[0] # Heuristic
                            # Try to find specific title element
                            title_el = card.find(['h2', 'h3', 'h4', 'a'])
                            if title_el: title = title_el.get_text().strip()
                            
                            job_url = self._extract_job_url_from_card(card, clean_url)
                            if not job_url: continue
                            
                            # Basic extraction from card
                            fallback_jobs.append({
                                "job_title": title,
                                "company": "Unknown - Extracted from Listing",
                                "location": "See Job Link",
                                "experience": "See Job Link",
                                "job_description": "Extracted via Selenium Fallback (Listing Only). Please visit link for details.",
                                "responsibilities": "",
                                "skills": [],
                                "source_url": job_url
                            })
                        except: continue
                
                # Also check for direct links if no cards found
                if len(fallback_jobs) == 0:
                     links = self._find_job_links(soup, clean_url)
                     for link in links[:10]:
                         fallback_jobs.append({
                                "job_title": "Job Link Found",
                                "company": "Unknown",
                                "location": "Unknown",
                                "experience": "Unknown",
                                "job_description": "Link extracted via Selenium Fallback",
                                "responsibilities": "",
                                "skills": [],
                                "source_url": link
                            })
            
            return fallback_jobs

    def _is_spa_url(self, url: str) -> bool:
        """Detect if URL is likely a SPA (Single Page Application)"""
        spa_indicators = [
            'myworkdayjobs.com',
            'taleo.net',
            'icims.com',
            'greenhouse.io',
            'lever.co',
            'startup.jobs', # ADDED for reliability
            '#/job',
            '#search'
        ]
        return any(indicator in url.lower() for indicator in spa_indicators)
    
    def _extract_with_selenium(self, url: str) -> str:
        """Extract HTML using Selenium for JavaScript-heavy pages"""
        # 2. URL BLOCKLIST CHECK
        if self.should_skip_url(url):
            return ""

        # 7. SELENIUM SAFETY RULE (Run ONCE per URL)
        if self.used_selenium_for_url.get(url):
            print(f"  [Selenium] Skipping repeated execution for: {url}")
            return ""
        self.used_selenium_for_url[url] = True
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        driver = None
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(60)
            
            print(f"  [Selenium] Loading: {url}")
            driver.get(url)
            
            # Wait for common job container elements to ensure loading
            print("  [Selenium] Waiting for job content...")
            try:
                # Wait for body first
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Robust wait for job-like elements
                job_selectors = [
                    "div[class*='job']", "li[class*='job']", "div[role='listitem']", 
                    "table", "div[class*='career']", "div[id*='job']",
                    "a[href*='job']", "h3", "h2"
                ]
                
                # Wait for at least one of these to appear
                end_time = time.time() + 10
                found_selector = False
                while time.time() < end_time:
                    for selector in job_selectors:
                        try:
                            if driver.find_elements(By.CSS_SELECTOR, selector):
                                found_selector = True
                                break
                        except: pass
                    if found_selector: break
                    time.sleep(0.5)
                
            except:
                print("  [Selenium] Warning: Timeout waiting for specific elements, proceeding with body")
            
            # Enhanced Scrolling Logic (Scroll to bottom to trigger lazy load)
            print("  [Selenium] Scrolling page to trigger lazy loading...")
            last_height = driver.execute_script("return document.body.scrollHeight")
            
            # Scroll down in chunks to simulate human reading and trigger observers
            for i in range(10): # increased scroll steps
                # Scroll by a chunk
                driver.execute_script("window.scrollBy(0, 800);")
                time.sleep(1.0) # Wait for content to load
                
                # Check if we've reached the bottom
                new_height = driver.execute_script("return document.body.scrollHeight")
                if driver.execute_script("return (window.innerHeight + window.scrollY) >= document.body.offsetHeight"):
                     # If at bottom, wait a bit and check for expansion
                     time.sleep(2)
                     new_height = driver.execute_script("return document.body.scrollHeight")
                     if new_height == last_height:
                         break 
                last_height = new_height
            
            # Additional small scroll up and down to trigger intersection observers
            driver.execute_script("window.scrollBy(0, -500);")
            time.sleep(0.5)
            driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(2)
            
            html = driver.page_source
            print(f"  [Selenium] Extracted {len(html)} bytes")
            return html
            
        except Exception as e:
            print(f"  [Selenium] Error: {e}")
            return ""
        finally:
            if driver:
                driver.quit()
    
    def _detect_and_fill_filters(self, soup: BeautifulSoup, base_url: str, country: str) -> Optional[str]:
        """
        STEP 3: Auto-fill Filters
        Detects job search forms and constructs a URL with location/query filters applied.
        """
        if not country: return None
        
        # simple check for inputs
        inputs = soup.find_all(['input', 'select'])
        has_location = False
        has_keyword = False
        
        for i in inputs:
            attr_str = str(i.attrs).lower()
            if 'location' in attr_str or 'place' in attr_str or 'country' in attr_str:
                has_location = True
            if 'keyword' in attr_str or 'search' in attr_str or 'q=' in attr_str:
                has_keyword = True
                
        if not (has_location or has_keyword):
            return None
            
        # If we found likely filters, try to construct a standard search URL
        # This is a heuristic: many sites use ?location=X or ?q=X
        # We try to guess the parameter name based on common patterns
        
        parsed = urlparse(base_url)
        query_params = {}
        
        if has_location:
            # Try common location params
            query_params['location'] = country
            query_params['loc'] = country
            query_params['country'] = country
            
        if has_keyword:
            query_params['q'] = country # Often searching country in keyword works too
            query_params['keywords'] = country
            
        # Construct new URL
        # We append these common params. Most modern sites will ignore unknown params.
        # This simulates "filling" the fields and submitting GET form.
        new_query = urlencode(query_params, doseq=True)
        
        # If the URL already has query params, we append/overwrite
        if parsed.query:
            return f"{base_url}&{new_query}"
        else:
            return f"{base_url}?{new_query}"

    async def _recursive_crawl(self, url: str, country: str, crawler, depth: int) -> List[Dict]:
        """Recursive function to handle navigation with Loop Safety and Data Quality"""
        # 8. DEPTH LIMIT
        MAX_DEPTH = 2
        if depth > MAX_DEPTH: 
            return []
            
        # 3. VISITED URL GUARD
        if url in self.visited_urls:
            return []
        self.visited_urls.add(url)
        
        # 2. URL BLOCKLIST CHECK
        if self.should_skip_url(url):
            return []

        if depth > 3: return [] # Legacy check kept for safety
        
        # LOOP SAFETY: Check if we've processed this specific URL or Gateway
        if url in self.seen_urls or url in self.visited_gateways:
            return []
        self.seen_urls.add(url)

        print(f"Accessing URL (Depth {depth}): {url}")
        
        # Detect if SPA
        is_spa = self._is_spa_url(url)
        
        try:
            if is_spa:
                result = await crawler.arun(
                    url=url,
                    word_count_threshold=10,
                    js_code=["const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms)); await delay(3000);"],
                    wait_for="css:body",
                    delay_before_return_html=2.0
                )
            else:
                result = await crawler.arun(url=url, word_count_threshold=10)
                
            if not result.success:
                print(f"Failed to load: {url}")
                return []
            
            soup = BeautifulSoup(result.html, 'html.parser')
            
            # Selenium Fallback for SPAs if content is minimal
            # Calculate metrics immediately to be safe
            text_content = soup.get_text(strip=True) if soup else ""
            word_count = len(text_content.split())
            
            # Selenium Fallback for SPAs if content is minimal or suspicious
            # Fix: Ensure word_count and text_content are defined before this check
            if is_spa or word_count < 100 or "enable javascript" in text_content.lower() or "please enable" in text_content.lower():
                print(f"  [Fallback] Triggering Selenium (Word Count: {word_count}, SPA: {is_spa})...")
                selenium_html = self._extract_with_selenium(url)
                if selenium_html:
                    soup = BeautifulSoup(selenium_html, 'html.parser')
                    # Re-calculate word count to verify improvement
                    text_content = soup.get_text(strip=True)
                    word_count = len(text_content.split())
                    print(f"  [Fallback] New Word Count: {word_count}")
                else:
                    print(f"  [Fallback] Selenium failed to extract content")
                    if word_count < 50: # Only return empty if we really have nothing
                        return []
            
            # CLASSIFY PAGE TYPE
            page_type = self._classify_page_type(soup, url)
            print(f"Page Classification: {page_type}")

            # 1. HANDLE CAREER INFO / MARKETING (Prioritize Rejection)
            if page_type == "CAREER_INFO":
                print("  Rejected: CAREER_INFO / Marketing Page")
                return []

            # 2. HANDLE JOB SEARCH APPS (SPA)
            if page_type == "JOB_SEARCH_APP":
                print("  Detected SPA / Job Search App. Attempting shallow extraction...")
                # Try specific extraction for simple SPAs, otherwise risk failing without JS
                # For now, we fallback to finding any links that look like jobs
                return await self._extract_from_job_listing(url, soup, country, crawler)

            # 3. HANDLE JOB DETAIL
            if page_type == "JOB_DETAIL":
                job_data = await self._extract_single_job(url, soup, crawler)
                if job_data and self._validate_job(job_data, country):
                    return [job_data]
                return []
            
            # 4. HANDLE JOB LISTING
            elif page_type == "JOB_LISTING":
                # 5. TERMINAL SUCCESS RULE
                extracted_jobs = await self._extract_from_job_listing(url, soup, country, crawler)
                
                # 6. ZERO-JOBS CHECK FIX
                # FALLBACK: If we thought it was a listing but found no valid jobs, 
                # check if it's actually a gateway with "Explore" buttons we missed
                if len(extracted_jobs) == 0:
                    print(f"  Zero jobs found on apparent JOB_LISTING. Checking for gateway links...")
                    category_links = self._find_category_links(soup, url)
                    if len(category_links) > 0:
                         print(f"  Reclassifying as CAREER_GATEWAY. Found {len(category_links)} links.")
                         all_jobs = []
                         for cat in category_links:
                            if cat['url'] not in self.visited_gateways:
                                print(f"  Fallback Navigating to: {cat['name']}")
                                jobs = await self._recursive_crawl(cat['url'], country, crawler, depth + 1)
                                all_jobs.extend(jobs)
                         return self._deduplicate_jobs(all_jobs)
                else: 
                     return extracted_jobs # Jobs found, stop crawling
                
                return extracted_jobs
            
            # 5. HANDLE CAREER GATEWAY (Strict Loop Safety)
            elif page_type == "CAREER_GATEWAY":
                # Mark as visited gateway so we NEVER re-enter this specific hub
                self.visited_gateways.add(url)
                
                category_links = self._find_category_links(soup, url)
                print(f"  Found {len(category_links)} categories to explore")
                
                all_jobs = []
                for cat in category_links:
                    # STRICT CHECK: Do not re-visit any child gateway that we've already seen
                    if cat['url'] not in self.visited_gateways:
                        print(f"  Navigating to category: {cat['name']}")
                        jobs = await self._recursive_crawl(cat['url'], country, crawler, depth + 1)
                        all_jobs.extend(jobs)
                
                # Check if we should also check for jobs on this page mixed with categories
                direct_jobs = await self._extract_from_job_listing(url, soup, country, crawler)
                all_jobs.extend(direct_jobs)
                
                return self._deduplicate_jobs(all_jobs)
            
            # 6. OTHER
            else:
                print("Unknown page type, attempting fallback extraction...")
                # Fallback: Try to find any job links anyway
                jobs = await self._extract_from_job_listing(url, soup, country, crawler)
                if len(jobs) == 0:
                    # Final Hail Mary: Check for categories even on OTHER pages
                    category_links = self._find_category_links(soup, url)
                    if len(category_links) > 0:
                        print(f"  No jobs found, trying potential category links: {len(category_links)}")
                        all_jobs = []
                        for cat in category_links:
                             if cat['url'] not in self.visited_gateways:
                                 jobs = await self._recursive_crawl(cat['url'], country, crawler, depth + 1)
                                 all_jobs.extend(jobs)
                        return self._deduplicate_jobs(all_jobs)
                return jobs

        except Exception as e:
            print(f"Error crawling {url}: {e}")
            return []

    
    def _classify_page_type(self, soup: BeautifulSoup, url: str = "") -> str:
        """
        Classify the page as one of:
        - JOB_DETAIL
        - JOB_LISTING
        - CAREER_GATEWAY
        - JOB_SEARCH_APP (Hash routing, SPA)
        - CAREER_INFO (Benefits, Culture - Hard Stop)
        - OTHER
        """
        text = soup.get_text().lower()
        
        # 1. HARD STOP: Check for Career Info / Marketing
        info_keywords = ["employee benefits", "our culture", "frequently asked questions", "diversity and inclusion", "life at", "why work here"]
        # Only classify as INFO if it lacks job signals
        if any(k in text for k in info_keywords) and len(self._find_job_cards(soup)) == 0:
             return "CAREER_INFO"

        # 2. Check for SPA / Job Search App
        if "#/job" in url or "#search" in url or "myworkday" in url or "taleo" in url:
             if len(self._find_job_cards(soup)) == 0:
                 return "JOB_SEARCH_APP"

        # 3. Check for Job Detail
        if self._is_single_job_page(soup):
            return "JOB_DETAIL"
        
        # 4. Check for Job Listing (Job Cards)
        job_cards = self._find_job_cards(soup)
        if len(job_cards) > 0:
            return "JOB_LISTING"
        
        # 5. Check for Career Gateway (Categories but NO job cards)
        categories = self._find_category_links(soup, "") 
        if len(categories) > 0:
            return "CAREER_GATEWAY"
            
        return "OTHER"

    def _find_category_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Detect category/hub links like 'Professionals', 'Students'"""
        categories = []
        
        # Keywords for hub categories
        hub_keywords = [
            "explore all opportunities", "explore all",
    # General career hubs
    "careers", "career", "career hub", "career site", "career portal",
    "career opportunities", "job opportunities", "work with us",
    "join us", "life at", "why join", "your career", "our people",

    # Audience / entry paths
    "professionals", "experienced professionals", "experienced",
    "students", "student opportunities",
    "graduates", "graduate programs", "graduate roles",
    "early careers", "early talent", "entry level", "junior roles",
    "internships", "interns", "internship program",
    "apprenticeships", "apprentice", "trainees",
    "campus", "campus hiring", "university", "college hiring",
    "new grads", "freshers",

    # Career paths / tracks
    "career paths", "career tracks", "career areas",
    "career options", "career categories", "job families",
    "job categories", "role categories",

    # Departments / functions (hub-style)
    "engineering", "technology", "software", "it",
    "data", "analytics", "ai", "machine learning",
    "product", "design", "ux", "ui",
    "sales", "marketing", "growth",
    "customer service", "customer support",
    "operations", "business operations",
    "finance", "accounting", "audit",
    "hr", "human resources", "people",
    "legal", "compliance",
    "administrative", "admin",
    "supply chain", "logistics",
    "manufacturing", "production",
    "security", "cybersecurity",

    # Industry / workforce hubs
    "corporate", "retail", "store jobs",
    "warehouse", "fulfillment", "distribution",
    "drivers", "delivery",
    "field jobs", "field service",
    "healthcare", "medical", "clinical",
    "aviation", "airlines",
    "banking", "financial services",
    "insurance",
    "energy", "utilities",
    "construction", "infrastructure",

    # Navigation / discovery triggers
    "explore careers", "explore jobs",
    "find your path", "find your career",
    "search jobs", "job search",
    "view all jobs", "browse jobs",
    "see roles", "see opportunities",
    "discover roles", "open roles"
]
        
        potential_links = soup.find_all('a', href=True)
        
        for link in potential_links:
            text = link.get_text().strip().lower()
            href = link.get('href', '')
            full_url = urljoin(base_url, href)
            
            if not href or href == '#' or href.startswith('javascript'):
                continue
                
            if any(x in href.lower() for x in ['login', 'signin', 'privacy', 'terms']):
                continue

            matched = False
            
            # Check 0: Priority ATS domains (Step 6)
            is_ats = any(domain in href.lower() for domain in ['myworkdayjobs.com', 'taleo.net', 'icims.com', 'greenhouse.io', 'lever.co', 'oraclecloud.com'])
            if is_ats: matched = True
            
            # Check 1: Text matches specific keywords
            for keyword in hub_keywords:
                if keyword in text and len(text) < 50: 
                    matched = True
                    break
            
            # Check 2: Element structure (Tile/Card) + text implies category
            if not matched:
                parent_classes = str(link.parent.get('class', [])).lower()
                if 'card' in parent_classes or 'tile' in parent_classes or 'category' in parent_classes:
                    if len(text) > 3 and len(text) < 50:
                        matched = True

            if matched:
                if not any(c['url'] == full_url for c in categories):
                    categories.append({
                        'name': text.title(),
                        'url': full_url
                    })
                
        return categories[:8] 

    def _deduplicate_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """Merge and deduplicate jobs"""
        unique_jobs = {}
        for job in jobs:
            key = job.get('job_id')
            if not key or key in unique_jobs:
                key = f"{job['job_title']}_{job['location']}_{job['company']}"
            
            if key not in unique_jobs:
                unique_jobs[key] = job
        
        return list(unique_jobs.values())

    
    def _get_job_detail_url(self, url: str) -> str:
        """RULE #1: Convert apply URLs to job detail URLs"""
        if '/apply' in url:
            return url.split('/apply')[0]
        if '/application' in url:
            return url.split('/application')[0]
        return url
    
    
    def _is_single_job_page(self, soup: BeautifulSoup) -> bool:
        """Detect if page is a single job detail page"""
        # Critical Fix: If it has the specific Pearson job list, it is NOT a single page
        if soup.find('ul', id='jobs') and len(soup.select('ul[id="jobs"] li')) > 0:
            return False

        job_detail_indicators = [
            soup.find(attrs={'data-testid': re.compile(r'job-detail', re.I)}),
            soup.find('section', role='main'),
            soup.find('article'),
            # soup.find('main'), # TOO BROAD - Removed to prevent false positives on listing pages
            soup.find('div', class_=re.compile(r'job-detail|position-detail|opening-detail', re.I))
        ]
        
        if any(job_detail_indicators):
            return True
        
        job_content = soup.find_all(text=re.compile(r'responsibilities|requirements|qualifications|job description', re.I))
        job_cards = self._find_job_cards(soup)
        
        # More flexible check: if significant job text and NOT a listing
        return len(job_content) > 2 and len(job_cards) == 0
    
    async def _extract_single_job(self, job_url: str, soup: BeautifulSoup, crawler) -> Optional[Dict]:
        """Extract from single job detail page"""
        # 4. JOB DETAIL DEDUPLICATION
        if job_url in self.visited_job_urls:
            return None
        self.visited_job_urls.add(job_url)

        if self._is_apply_page(soup):
            return None
        
        job_container = self._find_job_container(soup) or soup
        
        json_ld_data = self._extract_json_ld(soup)
        if json_ld_data:
            return self._create_job_from_json_ld(json_ld_data, job_url)
        
        title = self._extract_job_title(job_container)
        if self._is_invalid_title(title):
            return None
        
        location = self._extract_location(job_container)
        description = self._extract_description(job_container)
        responsibilities = self._extract_responsibilities(job_container)
        skills = self._extract_skills(description)
        company = self._extract_company(job_container, job_url)
        employment_type = self._extract_employment_type(job_container, description)
        
        return {
            "job_title": title,
            "company": company,
            "location": location,
            "experience": employment_type,
            "job_description": description,
            "responsibilities": responsibilities,
            "skills": skills,
            "source_url": job_url
        }
    
    def _is_apply_page(self, soup: BeautifulSoup) -> bool:
        """RULE #8: Detect apply/form pages"""
        forms = soup.find_all('form')
        if len(forms) > 1: return True
        
        apply_indicators = ['provide your contact information', 'upload resume', 'application form', 'recaptcha']
        page_text = soup.get_text().lower()
        return any(indicator in page_text for indicator in apply_indicators)
    
    def _find_job_container(self, soup: BeautifulSoup) -> Optional:
        container_selectors = [
            '[data-testid*="job-detail"]', 'section[role="main"]', 'article', 'main',
            'div[class*="job-detail"]', 'div[class*="position-detail"]',
            'div[class*="job-content"]', 'div[class*="opening-detail"]'
        ]
        for selector in container_selectors:
            container = soup.select_one(selector)
            if container: return container
        
        content_divs = soup.find_all('div', class_=re.compile(r'content|main|body', re.I))
        if content_divs:
            return max(content_divs, key=lambda x: len(x.get_text()))
        return None
    
    def _extract_json_ld(self, soup: BeautifulSoup) -> Optional[Dict]:
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, list): data = data[0]
                if data.get('@type') == 'JobPosting': return data
                if isinstance(data, dict):
                    for v in data.values():
                        if isinstance(v, dict) and v.get('@type') == 'JobPosting': return v
            except: continue
        return None
    
    def _create_job_from_json_ld(self, json_data: Dict, job_url: str) -> Dict:
        title = json_data.get('title', '')
        
        location = "Location not specified"
        job_location = json_data.get('jobLocation', {})
        if isinstance(job_location, dict):
            address = job_location.get('address', {})
            if isinstance(address, dict):
                parts = []
                for p in [address.get('addressLocality'), address.get('addressRegion'), address.get('addressCountry')]:
                    if isinstance(p, str) and p:
                        parts.append(p)
                    elif isinstance(p, dict) and p.get('name'):
                        parts.append(p.get('name'))
                location = ", ".join(parts)
            elif isinstance(address, str): location = address
        elif isinstance(job_location, str): location = job_location
        
        company = "Company not specified"
        hiring_org = json_data.get('hiringOrganization', {})
        if isinstance(hiring_org, dict): company = hiring_org.get('name', company)
        
        description = json_data.get('description', 'Job description not available')
        employment_type = json_data.get('employmentType', 'Full-time')
        
        return {
            "job_title": title,
            "company": company,
            "location": location,
            "experience": employment_type,
            "job_description": description,
            "responsibilities": "",
            "skills": self._extract_skills(description),
            "source_url": job_url
        }
    
    def _extract_job_title(self, container) -> str:
        # Workday-specific selectors (priority)
        workday_selectors = [
            'h1[data-automation-id="jobPostingHeader"]',
            'h2[class*="JobTitle"]',
            '[data-automation-id*="jobTitle"]'
        ]
        for selector in workday_selectors:
            element = container.select_one(selector)
            if element:
                title = element.get_text().strip()
                if title and not self._is_invalid_title(title): return title
        
        # Generic selectors
        title_selectors = [
            'h1[data-testid*="job-title"]', 'h1[class*="job-title"]',
            'h1[class*="position-title"]', '[data-testid*="job-title"]',
            'h1', 'h2[class*="job"]', '[role="heading"][aria-level="1"]'
        ]
        for selector in title_selectors:
            element = container.select_one(selector)
            if element:
                title = element.get_text().strip()
                if title and not self._is_invalid_title(title): return title
        return "Job Title Not Available"

    def _validate_job(self, job_data: Dict, country: str) -> bool:
        """RULE #5: Validation (Hard Fail)"""
        if not job_data: return False
        
        # Calculate Confidence Score
        score = self._score_job(job_data)
        if score < 3:
            return False
            
        if country and not self._validate_country(job_data, country):
            return False

        return True

    def _score_job(self, job_data: Dict) -> int:
        """Step 8: Job Confidence Scoring"""
        score = 0
        title = job_data.get('job_title', '').lower()
        desc = job_data.get('job_description', '').lower()
        
        # Positive Signals
        if len(title) > 3 and not self._is_invalid_title(title): score += 2
        if len(desc) > 200: score += 2
        if job_data.get('skills') and len(job_data.get('skills')) > 0: score += 1
        
        loc = job_data.get('location', '')
        if loc and loc.lower() != 'location not specified': score += 1
        
        # Specific Title Boosts
        if any(role in title for role in ['engineer', 'developer', 'manager', 'analyst', 'consultant', 'specialist', 'director', 'intern']):
            score += 1

        # Negative Signals
        if any(x in desc for x in ["employee benefits", "our culture", "frequently asked questions"]): 
            score -= 3
        if "application form" in desc or "upload resume" in desc: 
            score -= 5
            
        return score

    
    def _is_invalid_title(self, title: str) -> bool:
        if not title or len(title) < 3: return True
        # STRICT rejection of form/action titles
        invalid = [
            'apply', 'provide', 'upload resume', 'join us', 'login', 'create account', 
            'careers', 'jobs', 'work with us', 'search', 'results', 'filter',
            'please wait', 'loading', 'application', 'job title not available',
            'profile', 'member', 'blog', 'news', 'events'
        ]
        title_lower = title.lower()
        return any(i in title_lower for i in invalid)
    
    def _extract_location(self, container) -> str:
        location_selectors = [
            '[data-testid*="location"]', '[class*="location"]',
            '[class*="job-location"]', 'span[class*="location"]',
            'div[class*="location"]'
        ]
        # 1. Workday-specific selectors (priority)
        workday_location_selectors = [
            '[data-automation-id="locations"]',
            'div[class*="Location"]',
            'span[data-automation-id*="location"]'
        ]
        for selector in workday_location_selectors:
            el = container.select_one(selector)
            if el:
                loc = el.get_text().strip()
                if self._is_valid_location(loc): return loc
        
        # 2. Proximity Check (Sibling of Title)
        title_elem = None
        for sel in ['h1', 'h2', 'h3', '[data-testid*="job-title"]', '[class*="job-title"]']:
            title_elem = container.select_one(sel)
            if title_elem: break
            
        if title_elem:
            # Check next sibling
            sib = title_elem.find_next_sibling()
            if sib:
                sib_text = sib.get_text().strip()
                if len(sib_text) > 2 and len(sib_text) < 50 and not any(x in sib_text.lower() for x in ['apply', 'job']):
                     # Simple heuristic: often location is just "City, Country"
                     if ',' in sib_text or self._is_valid_location(sib_text):
                        return sib_text

        # 2. Selector Check
        for selector in location_selectors:
            el = container.select_one(selector)
            if el:
                loc = el.get_text().strip()
                if self._is_valid_location(loc): return loc
        
        text = container.get_text()
        patterns = [
            r'\b([A-Z][a-z]+,\s*[A-Z]{2})\b',
            r'\b([A-Z][a-z]+,\s*[A-Z][a-z]+)\b',
            r'\b(Remote)\b'
        ]
        for p in patterns:
            match = re.search(p, text)
            if match: return match.group(1)
            
        return "Location not specified"
        
    def _is_valid_location(self, location: str) -> bool:
        if not location or len(location) < 3: return False
        invalid = ['location', 'city', 'state', 'country', 'where', 'anywhere']
        return location.lower() not in invalid
    
    def _extract_description(self, container) -> str:
        # Workday-specific selectors (priority)
        workday_selectors = [
            '[data-automation-id="jobPostingDescription"]',
            'div[class*="JobDescription"]',
            'div[data-automation-id*="description"]'
        ]
        
        for selector in workday_selectors:
            el = container.select_one(selector)
            if el: return self._clean_description(el.get_text(separator=' ', strip=True))
        
        # Generic selectors
        desc_selectors = [
            '[data-testid*="description"]', '[class*="job-description"]', 
            '[class*="description"]', '[class*="job-details"]', '[class*="job-content"]',
            'div[id*="description"]', 'section[id*="description"]'
        ]
        for selector in desc_selectors:
            el = container.select_one(selector)
            if el: return self._clean_description(el.get_text(separator=' ', strip=True))
        
        # Fallback - look for responsibility/requirement headers
        job_sections = container.find_all(text=re.compile(r'responsibilities|requirements|qualifications|about|role overview', re.I))
        if job_sections:
            parent = job_sections[0].find_parent(['div', 'section', 'article'])
            if parent: return self._clean_description(parent.get_text(separator=' ', strip=True))

        return "Job description not available"
    
    def _extract_responsibilities(self, container) -> str:
        """Extract responsibilities section from job description"""
        # Look for responsibilities section
        resp_headers = container.find_all(['h2', 'h3', 'h4', 'b', 'strong'], 
                                         string=re.compile(r'responsibilit', re.I))
        
        if resp_headers:
            header = resp_headers[0]
            # Get the next sibling (usually a ul or p)
            resp_section = header.find_next_sibling(['ul', 'div', 'p'])
            if resp_section:
                text = resp_section.get_text(separator=' • ', strip=True)
                # Clean HTML if needed
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(text, 'html.parser')
                text = soup.get_text(separator=' • ', strip=True)
                return text[:500] if len(text) > 500 else text
        
        return ""
    
    def _clean_description(self, text: str) -> str:
        # Strip HTML tags if present
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(text, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'apply now.*?$', '', text, flags=re.IGNORECASE)
        return text.strip()
    
    def _extract_skills(self, description: str) -> List[str]:
        skills_patterns = [
            r'\b(Python|Java|JavaScript|TypeScript|C\+\+|C#|Ruby|PHP|Go|Rust|Kotlin|Swift)\b',
            r'\b(React|Angular|Vue|Django|Flask|Spring|Node\.js|Express)\b',
            r'\b(AWS|Azure|GCP|Docker|Kubernetes|Git|Jenkins)\b',
            r'\b(SQL|MySQL|PostgreSQL|MongoDB|Redis|Elasticsearch)\b'
        ]
        skills = set()
        for p in skills_patterns:
            skills.update(re.findall(p, description, re.IGNORECASE))
        return list(skills)
    
    def _extract_company(self, container, job_url: str) -> str:
        selectors = ['[data-testid*="company"]', '[class*="company"]', '[class*="employer"]']
        for s in selectors:
            el = container.select_one(s)
            if el and len(el.get_text()) > 1: return el.get_text().strip()
        
        domain = urlparse(job_url).netloc
        return domain.replace('www.', '').split('.')[0].title()

    def _extract_employment_type(self, container, description: str) -> str:
        text = (container.get_text() + ' ' + description).lower()
        
        # Check for specific "Workplace Type" labels first
        if 'workplace type: remote' in text: return 'Remote'
        if 'workplace type: on-site' in text or 'workplace type: onsite' in text: return 'On-site'
        if 'workplace type: hybrid' in text: return 'Hybrid'
        
        if 'remote' in text: return 'Remote'
        if 'part-time' in text or 'part time' in text: return 'Part-time'
        if 'contract' in text or 'temporary' in text: return 'Contract'
        return 'Full-time'

    async def _extract_from_job_listing(self, site_url: str, soup: BeautifulSoup, country: str, crawler) -> List[Dict]:
        job_cards = self._find_job_cards(soup)
        jobs = []
        visited = set()
        
        print(f"  Processing Job Listing. Found {len(job_cards)} cards.")
        
        for card in job_cards[:15]:
            job_url = self._extract_job_url_from_card(card, site_url)
            if job_url and job_url not in visited and not self._is_apply_url(job_url):
                visited.add(job_url)
                print(f"    Scraping card: {job_url}")
                job_data = await self._extract_single_job_from_url(job_url, crawler)
                if job_data and self._validate_job(job_data, country):
                    jobs.append(job_data)
        
        # Fallback to direct links if needed
        if len(jobs) < 3:
             links = self._find_job_links(soup, site_url)
             for link in links[:10]:
                 if link not in visited and not self._is_apply_url(link):
                     visited.add(link)
                     print(f"    Scraping link: {link}")
                     job_data = await self._extract_single_job_from_url(link, crawler)
                     if job_data and self._validate_job(job_data, country):
                         jobs.append(job_data)

        return jobs

    def _find_job_cards(self, soup: BeautifulSoup) -> List:
        card_selectors = [
            'ul[id="jobs"] li', 'li[class*="jobs-list-item"]', # Specific Pearson/similar Fix
            '[data-testid*="job"]', '[class*="job-card"]', '[class*="job-item"]', 
            '[class*="position"]', 'article', 'li[class*="job"]', 
            'div[class*="card"]', 'div[class*="item"]', 'div[class*="listing"]',
            'tr[class*="job"]', '.job-list-item', 
            # New generic selectors for lists
            'li', '.search-result', '.list-row', '.row'
        ]
        cards = []
        seen_texts = set()
        
        # 1. Try specific selectors first
        for sel in card_selectors:
            elements = soup.select(sel)
            for elem in elements:
                # For generic tags like <li> or .row, require strict validation
                is_generic = sel in ['li', '.row', '.list-row']
                if is_generic and not self._is_definitely_job_card(elem):
                    continue
                    
                text = elem.get_text().strip()
                if text and text not in seen_texts and self._is_valid_job_card(elem):
                    cards.append(elem)
                    seen_texts.add(text)
        
        return cards[:20]

    def _is_definitely_job_card(self, element) -> bool:
        """Stronger check for generic elements"""
        text = element.get_text().lower()
        if len(text) > 500: return False # Too big
        
        # Must contain obvious job signals
        signals = ['workplace type', 'location:', 'posted', 'apply', 'job id', 'employment type']
        return any(s in text for s in signals)

    def _is_valid_job_card(self, element) -> bool:
        text = element.get_text().lower()
        if len(text) < 20: return False
        if not element.find('a'): return False
        
        # Reject Blogs / News
        if any(x in text for x in ['min read', 'read more', 'posted by', 'blog', 'article']):
            return False
            
        invalid = ['navigation', 'footer', 'header', 'menu']
        return not any(i in text for i in invalid)

    def _extract_job_url_from_card(self, card, base_url: str) -> Optional[str]:
        link = card.find('a')
        if link and link.get('href'):
            return self._get_job_detail_url(urljoin(base_url, link['href']))
        return None

    def _find_job_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        links = []
        seen = set()
        for a in soup.find_all('a', href=True):
            href = a.get('href', '')
            url = urljoin(base_url, href)
            if url in seen: continue
            
            # Expanded checks
            if '/job/' in href or '/career/' in href or '/position/' in href or '/vacancy/' in href:
               links.append(self._get_job_detail_url(url))
               seen.add(url)
            elif any(k in a.get_text().lower() for k in ['engineer', 'manager', 'developer']) and len(a.get_text()) < 50:
               links.append(self._get_job_detail_url(url))
               seen.add(url)
               
        return links

    def _is_apply_url(self, url: str) -> bool:
        """Check if URL is apply page"""
        return '/apply' in url or '/application' in url

    async def _extract_single_job_from_url(self, job_url: str, crawler) -> Optional[Dict]:
        try:
            is_spa = self._is_spa_url(job_url)
            
            if is_spa:
                result = await crawler.arun(
                    url=job_url,
                    word_count_threshold=50,
                    js_code=["const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms)); await delay(3000);"],
                    wait_for="css:body",
                    delay_before_return_html=2.0
                )
            else:
                result = await crawler.arun(url=job_url, word_count_threshold=50)
                
            if result.success:
                return await self._extract_single_job(job_url, BeautifulSoup(result.html, 'html.parser'), crawler)
        except: pass
        return None

    def _validate_country(self, job_data: Dict, country: str) -> bool:
        loc = job_data.get('location', '').lower()
        country_lower = country.lower()
        return (country_lower in loc or 'remote' in loc or 
                'not specified' in loc or 'worldwide' in loc)

    def _validate_job(self, job_data: Dict, country: str) -> bool:
        """Validate job against all quality rules"""
        # 1. Basic Schema & Ad Filter
        if not is_valid_job(job_data):
            return False
            
        # 2. Strict Role Check (Internship)
        if not matches_target_role(job_data.get('job_title', ''), mode="internship"):
            return False
            
        # 3. Country Check
        return self._validate_country(job_data, country)

# Replace the old scraper
class GenericJobScraper(ExpertJobScraper):
    pass