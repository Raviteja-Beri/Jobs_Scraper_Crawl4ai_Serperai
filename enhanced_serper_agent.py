"""
Enhanced Serper API integration for automatic job detection with complete JSON profiles
"""
import os
import requests
import asyncio
from typing import List, Dict, Optional
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import re
import json
from job_database import JobDatabase
from scraper_filters import is_valid_job, matches_target_role, sanitize_url

class EnhancedJobExtractionAgent:
    """Enhanced agent that creates complete JSON job profiles"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('SERPER_API_KEY')
        if not self.api_key:
            raise ValueError("SERPER_API_KEY not found")
        
        self.base_url = "https://google.serper.dev/search"
        self.headers = {'X-API-KEY': self.api_key, 'Content-Type': 'application/json'}
        self.db = JobDatabase()
    
    async def auto_extract_jobs(self, country: str, max_companies: int = 5) -> Dict:
        """Extract complete job profiles by country and save to database"""
        print(f"Starting enhanced job extraction for {country}...")
        
        # Clear existing data for this country first
        self.db.clear_all_jobs_for_country(country)
        
        # Find companies
        companies = self._find_companies(country, max_companies)
        if not companies:
            return {"error": f"No companies found in {country}"}
        
        all_jobs = []
        results = {}
        
        # Extract complete job profiles from each company
        for company in companies:
            print(f"Processing {company['name']}...")
            jobs = await self._extract_complete_job_profiles(company, country)
            
            if jobs:
                # Save complete job profiles to database
                saved_count = self._save_job_profiles(jobs, country)
                self.db.save_company(company['name'], company['url'], country, len(jobs))
                
                results[company['name']] = {
                    "jobs_found": len(jobs),
                    "jobs_saved": saved_count,
                    "website": company['url'],
                    "sample_job": jobs[0] if jobs else None  # Show sample structure
                }
                all_jobs.extend(jobs)
                print(f"  Extracted {len(jobs)} complete job profiles")
            else:
                results[company['name']] = {"error": "No jobs extracted"}
        
        return {
            "country": country,
            "total_companies": len(companies),
            "total_jobs": len(all_jobs),
            "companies": results,
            "job_profiles_sample": all_jobs[:2] if all_jobs else [],  # Show sample profiles
            "database_stats": self.db.get_stats()
        }
    
    def _find_companies(self, country: str, limit: int = 5) -> List[Dict]:
        """Find companies hiring in country using Serper API with pagination"""
        base_query = f"jobs careers hiring {country} site:careers OR site:jobs"
        
        companies = []
        seen_domains = set()
        start = 0
        batch_size = 20  # Safe batch size for Serper
        
        while len(companies) < limit:
            try:
                # Calculate how many more we need
                remaining = limit - len(companies)
                # Don't fetch more than batch_size at a time
                num_to_fetch = min(remaining * 2, batch_size) 
                
                print(f"  Searching batch: start={start}, num={num_to_fetch}...")
                
                payload = {
                    "q": base_query,
                    "num": num_to_fetch,
                    "start": start
                }
                
                response = requests.post(self.base_url, json=payload, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                
                new_companies_found = False
                for result in data.get('organic', []):
                    if len(companies) >= limit:
                        break
                    
                    link = result.get('link', '')
                    title = result.get('title', '')
                    domain = urlparse(link).netloc.lower()
                    
                    if domain not in seen_domains and self._is_career_site(title, link):
                        companies.append({
                            'name': self._extract_company_name(domain, title),
                            'url': link,
                            'domain': domain
                        })
                        seen_domains.add(domain)
                        new_companies_found = True
                
                # If no new companies found in this batch, likely exhausted results
                if not new_companies_found and len(data.get('organic', [])) < num_to_fetch:
                    break
                    
                start += num_to_fetch
                
                # Avoid infinite loops or excessive API calls
                if start > limit * 5: 
                    break
                    
            except Exception as e:
                print(f"Error finding companies in batch: {e}")
                break
                
        return companies
    
    async def _extract_complete_job_profiles(self, company: Dict, country: str) -> List[Dict]:
        """Extract complete job profiles using generic multi-step scraper"""
        try:
            from expert_job_scraper import ExpertJobScraper
            scraper = ExpertJobScraper()
            
            print(f"  Using EXPERT scraper for {company['name']}...")
            jobs = await scraper.extract_jobs_from_site(company['url'], country)
            
            if not jobs:
                print(f"  No valid jobs found for {company['name']}")
                return []
            
            print(f"  Extracted {len(jobs)} validated job profiles")
            return jobs
            
        except Exception as e:
            print(f"  Error extracting from {company['name']}: {str(e)[:50]}")
            return []
    
    def _extract_job_urls(self, soup: BeautifulSoup, company: Dict) -> List[str]:
        """Extract job detail page URLs from company career page"""
        job_urls = set()
        
        # Common job link patterns
        job_link_selectors = [
            'a[href*="/job/"]',
            'a[href*="/jobs/"]', 
            'a[href*="/career/"]',
            'a[href*="/careers/"]',
            'a[href*="/position/"]',
            'a[href*="/opening/"]',
            'a[class*="job"]',
            'a[data-testid*="job"]'
        ]
        
        for selector in job_link_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href', '')
                if href and self._is_valid_job_url(href):
                    full_url = urljoin(company['url'], href)
                    job_urls.add(full_url)
        
        return list(job_urls)
    
    def _is_valid_job_url(self, href: str) -> bool:
        """Validate if URL is a legitimate job detail page"""
        if not href:
            return False
        
        # Must contain job-related path segments
        job_indicators = ['job', 'career', 'position', 'opening', 'vacancy']
        href_lower = href.lower()
        
        if not any(indicator in href_lower for indicator in job_indicators):
            return False
        
        # Should not be generic pages
        exclude_patterns = [
            'search', 'filter', 'category', 'department', 
            'location', 'apply-now', 'login', 'register'
        ]
        
        if any(pattern in href_lower for pattern in exclude_patterns):
            return False
        
        # Should have some form of ID or specific identifier
        if re.search(r'\\d+|[a-f0-9]{8,}', href):
            return True
        
        # Or be a specific job path
        if re.search(r'/job[s]?/[^/]+/?$', href, re.IGNORECASE):
            return True
        
        return False
    
    def _create_basic_job_profile(self, job_url: str, company: str, domain: str) -> Dict:
        """Create a basic job profile with complete JSON structure"""
        from datetime import datetime
        import hashlib
        
        # Generate job ID
        job_id = f"job_{hashlib.md5(job_url.encode()).hexdigest()[:12]}"
        
        # Extract basic info from URL
        title = self._extract_title_from_url(job_url)
        
        return {
            "job_id": job_id,
            "job_title": title,
            "company": company,
            "location": "Location not specified",
            "employment_type": "Full-time",
            "job_description": "Job description not available - extracted from URL only",
            "skills": [],
            "apply_url": job_url,
            "source_url": job_url,
            "scraped_from": domain,
            "scraped_at": datetime.now().isoformat(),
            "apply_page_data": {
                "job_id": "",
                "location": "",
                "skills": []
            }
        }
    
    def _extract_title_from_url(self, url: str) -> str:
        """Extract job title from URL"""
        # Try to extract title from URL path
        parts = url.split('/')
        for part in reversed(parts):
            if part and len(part) > 3 and not part.isdigit():
                # Clean up the part
                title = part.replace('-', ' ').replace('_', ' ').title()
                if len(title) > 5:
                    return title
        
        return "Job Title Not Available"
    
    def _is_career_site(self, title: str, link: str) -> bool:
        """Check if the link is a career/jobs site"""
        career_indicators = ['career', 'job', 'hiring', 'work', 'employment', 'opportunity']
        title_lower = title.lower()
        link_lower = link.lower()
        
        # Check title for career indicators
        if any(indicator in title_lower for indicator in career_indicators):
            return True
        
        # Check URL for career paths
        if any(indicator in link_lower for indicator in ['career', 'job', 'hiring']):
            return True
        
        return False
    
    def _extract_company_name(self, domain: str, title: str) -> str:
        """Extract clean company name from domain and title"""
        # Remove common domain suffixes
        domain_clean = domain.replace('www.', '').split('.')[0]
        
        # Try to extract from title first
        title_words = title.split()
        if len(title_words) > 0:
            # Look for company name patterns in title
            for word in title_words:
                if len(word) > 3 and word.isalpha():
                    return word.title()
        
        # Fallback to domain
        return domain_clean.title()
    
    
    def _save_job_profiles(self, job_profiles: List[Dict], country: str) -> int:
        """Save complete job profiles to database with enhanced structure and validation"""
        saved_count = 0
        
        for profile in job_profiles:
            try:
                # 1. QUALITY CHECK
                if not is_valid_job(profile):
                    print(f"  Skipping invalid job/ad: {profile.get('job_title', 'Unknown')}")
                    continue
                
                # 2. ROLE CHECK (Strict Internship Mode)
                title = profile.get('job_title', '')
                if not matches_target_role(title, mode="internship"):
                    # print(f"  Skipping non-internship role: {title}") # verbose
                    continue

                # Convert complete job profile to database format
                job_data = {
                    'job_id': profile.get('job_id', ''),
                    'company': profile.get('company', ''),
                    'title': title,
                    'location': profile.get('location', ''),
                    'experience': profile.get('employment_type', ''),
                    'type': profile.get('employment_type', ''),
                    'description': profile.get('job_description', ''),
                    'skills': ', '.join(profile.get('skills', [])),
                    'apply_link': sanitize_url(profile.get('apply_url', '')) or ''
                }
                
                # Save using existing database method
                saved = self.db.save_jobs([job_data], country)
                saved_count += saved
                
            except Exception as e:
                print(f"Error saving job profile: {e}")
        
        return saved_count