"""
Scraper Filters - Quality Control Module
"""
import re
from urllib.parse import urlparse

def sanitize_url(url: str) -> str:
    """Sanitize URL and return None if invalid protocol"""
    if not url: return None
    
    # Remove whitespace
    url = url.strip()
    
    # Block invalid protocols
    if url.lower().startswith(('mailto:', 'tel:', 'javascript:', 'data:', '#')):
        return None
        
    # Ensure http/https
    if not url.lower().startswith(('http://', 'https://')):
        return None
        
    return url

def matches_target_role(title: str, mode: str = "internship") -> bool:
    """Check if title matches target role requirements STRICTLY"""
    if not title: return False
    title_lower = title.lower()
    
    if mode == "internship":
        # Must contain at least one positive signal
        allowed_terms = ['intern', 'internship', 'co-op', 'trainee', 'graduate', 'student', 'apprentice', 'fellowship']
        if not any(term in title_lower for term in allowed_terms):
            return False
            
        # Must NOT contain negative signals (Seniority)
        forbidden_terms = [
            'senior', 'staff', 'lead', 'principal', 'architect', 
            'manager', 'director', 'vp', 'head of', 'executive',
            'sr.', 'sr ', 'chief', 'partner'
        ]
        # Allow "Project Manager Intern" but not "Project Manager"
        # So we check if the forbidden term exists standing alone or as a prefix
        # Simple check: if any forbidden term is present
        if any(term in title_lower for term in forbidden_terms):
            # Exception: "Senior Intern" (rare but possible). 
            # But usually Senior X Intern is okay? User said REJECT titles containing Senior.
            return False
            
    return True

def is_valid_job(job_dict: dict) -> bool:
    """
    Validate a job dictionary against quality rules.
    Returns True if valid, False if it should be discarded.
    """
    if not job_dict: return False
    
    title = job_dict.get('job_title', '') or job_dict.get('title', '')
    company = job_dict.get('company', '')
    desc = job_dict.get('job_description', '') or job_dict.get('description', '')
    
    # Rule 1: Missing critical fields
    if not title or not company:
        return False
        
    # Rule 2: Ad / Privacy / Cookie detection in Title or Company
    junk_terms = [
        'youradchoices', 'privacy policy', 'cookie policy', 'terms of use',
        'do not sell my info', 'opt out', 'digital advertising alliance',
        'interest-based ads', 'browser cookies', 'javascript error'
    ]
    
    text_check = (title + " " + company).lower()
    if any(term in text_check for term in junk_terms):
        return False
        
    # Rule 3: Description Quality
    if len(desc) < 50: # Too short
        return False
        
    # Check for non-job descriptions (e.g. just a privacy policy text)
    if "privacy policy" in desc.lower() and len(desc) < 500 and "responsibilities" not in desc.lower():
        return False
        
    return True
