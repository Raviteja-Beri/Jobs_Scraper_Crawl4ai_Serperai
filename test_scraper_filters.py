import unittest
from scraper_filters import is_valid_job, matches_target_role, sanitize_url

class TestScraperFilters(unittest.TestCase):
    
    def test_sanitize_url(self):
        # Valid URLs
        self.assertEqual(sanitize_url("https://example.com"), "https://example.com")
        self.assertEqual(sanitize_url("http://test.co.uk/job"), "http://test.co.uk/job")
        
        # Whitespace
        self.assertEqual(sanitize_url("  https://example.com  "), "https://example.com")
        
        # Invalid Protocols (Should accept http/https only)
        self.assertIsNone(sanitize_url("mailto:user@example.com"))
        self.assertIsNone(sanitize_url("tel:1234567890"))
        self.assertIsNone(sanitize_url("javascript:void(0)"))
        self.assertIsNone(sanitize_url("data:image/png;base64,..."))
        self.assertIsNone(sanitize_url("#section"))
        self.assertIsNone(sanitize_url("ftp://example.com")) # Only http/https allowed per logic

    def test_matches_target_role_internship(self):
        # internship mode
        self.assertTrue(matches_target_role("Software Engineering Intern", mode="internship"))
        self.assertTrue(matches_target_role("Data Science Co-op", mode="internship"))
        self.assertTrue(matches_target_role("Summer 2026 Internet Trainee", mode="internship"))
        self.assertTrue(matches_target_role("Graduate Researcher", mode="internship"))
        
        # Direct misses
        self.assertFalse(matches_target_role("Software Engineer", mode="internship"))
        
        # Negative signals (Seniority)
        self.assertFalse(matches_target_role("Senior Software Intern", mode="internship"))
        self.assertFalse(matches_target_role("Lead Intern", mode="internship"))
        self.assertFalse(matches_target_role("Director of Interns", mode="internship"))
        self.assertFalse(matches_target_role("Intern Manager", mode="internship"))
        
        # Case insensitivity
        self.assertTrue(matches_target_role("summer intern", mode="internship"))

    def test_is_valid_job(self):
        # Valid Job
        valid_job = {
            'job_title': 'Software Engineer',
            'company': 'Tech Corp',
            'job_description': 'Writing code and fixing bugs. ' * 10
        }
        self.assertTrue(is_valid_job(valid_job))
        
        # Missing fields
        self.assertFalse(is_valid_job({'job_title': 'Engineer'}))
        self.assertFalse(is_valid_job({'company': 'Corp'}))
        
        # Ad / Privacy detection
        ad_job = {
            'job_title': 'YourAdChoices Statistics',
            'company': 'Ad Alliance',
            'job_description': 'Some text...' * 5
        }
        self.assertFalse(is_valid_job(ad_job))
        
        privacy_job = {
            'job_title': 'Privacy Policy',
            'company': 'Legal Dept',
            'job_description': 'This is the privacy policy...' * 5
        }
        self.assertFalse(is_valid_job(privacy_job))
        
        # Short description
        short_job = {
            'job_title': 'Valid Title',
            'company': 'Valid Co',
            'job_description': 'Too short'
        }
        self.assertFalse(is_valid_job(short_job))

if __name__ == '__main__':
    unittest.main()
