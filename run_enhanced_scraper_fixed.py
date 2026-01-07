"""
Enhanced Job Scraper Runner - Production Integration
"""
import asyncio
import sys
import os
import io

# Fix Windows console encoding
sys.stdout.reconfigure(encoding='utf-8')

from enhanced_serper_agent import EnhancedJobExtractionAgent
from job_database import JobDatabase
from dotenv import load_dotenv
load_dotenv()

async def run_enhanced_scraper(country: str, max_companies: int = 5):
    """Run the enhanced job scraper for a specific country"""
    
    print(f"Starting Enhanced Job Scraper for {country}")
    print("=" * 60)
    
    # Check for API key
    if not os.getenv('SERPER_API_KEY'):
        print("Error: SERPER_API_KEY environment variable not set")
        print("Please set your Serper API key: set SERPER_API_KEY=your_key_here")
        return
    
    try:
        # Initialize enhanced agent
        agent = EnhancedJobExtractionAgent()
        
        # Extract jobs with complete profiles
        print(f"Searching for companies in {country}...")
        result = await agent.auto_extract_jobs(country, max_companies)
        
        if 'error' in result:
            print(f"Error: {result['error']}")
            return
        
        # Display results
        print(f"\nEXTRACTION SUMMARY")
        print("-" * 30)
        print(f"Country: {result['country']}")
        print(f"Companies Processed: {result['total_companies']}")
        print(f"Total Jobs Extracted: {result['total_jobs']}")
        
        # Show per-company results
        companies = result.get('companies', {})
        successful_companies = 0
        
        for company_name, company_data in companies.items():
            if 'error' not in company_data:
                successful_companies += 1
                print(f"\nSUCCESS: {company_name}")
                print(f"   Jobs: {company_data['jobs_found']}")
                print(f"   Website: {company_data['website']}")
            else:
                print(f"\nFAILED: {company_name}: {company_data['error']}")
        
        print(f"\nSUCCESS RATE: {successful_companies}/{result['total_companies']} companies")
        
        # Database verification (Simple Stats)
        db = JobDatabase()
        stats = db.get_stats()
        print(f"\nDATABASE STATUS")
        print("-" * 20)
        print(f"Total Jobs in DB: {stats['total_jobs']}")
        print(f"Jobs for {country}: {stats['jobs_by_country'].get(country, 0)}")
        
        # Removed get_complete_job_profiles call as it was deprecated
        # The data is safely stored in the 'jobs' table.
        
        print(f"\nEnhanced job extraction completed successfully!")
        print(f"All jobs are stored as complete JSON profiles in the database.")
        
    except Exception as e:
        print(f"Error during extraction: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python run_enhanced_scraper.py <country> [max_companies]")
        print("Example: python run_enhanced_scraper.py Singapore 5")
        return
    
    country = sys.argv[1]
    max_companies = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    
    # Run the enhanced scraper
    asyncio.run(run_enhanced_scraper(country, max_companies))

if __name__ == "__main__":
    main()