"""
Clear all existing data from the database
"""

from job_database import JobDatabase

def clear_all_data():
    """Clear all existing data from the database"""
    
    db = JobDatabase()
    
    # Get current stats
    stats = db.get_stats()
    print("CURRENT DATABASE STATUS:")
    print(f"Total Jobs: {stats['total_jobs']}")
    print(f"Total Companies: {stats['total_companies']}")
    for country, count in stats['jobs_by_country'].items():
        print(f"Jobs for {country}: {count}")
    
    print("\nClearing all data...")
    
    # Clear all data
    import sqlite3
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    
    # Delete all jobs
    cursor.execute('DELETE FROM jobs')
    jobs_deleted = cursor.rowcount
    
    # Delete all companies
    cursor.execute('DELETE FROM companies')
    companies_deleted = cursor.rowcount
    
    # Reset auto-increment counters
    cursor.execute('DELETE FROM sqlite_sequence WHERE name="jobs"')
    cursor.execute('DELETE FROM sqlite_sequence WHERE name="companies"')
    
    conn.commit()
    conn.close()
    
    print(f"Deleted {jobs_deleted} jobs")
    print(f"Deleted {companies_deleted} companies")
    print("Reset auto-increment counters")
    
    # Verify clearing
    new_stats = db.get_stats()
    print("\nNEW DATABASE STATUS:")
    print(f"Total Jobs: {new_stats['total_jobs']}")
    print(f"Total Companies: {new_stats['total_companies']}")
    
    print("\nDatabase cleared successfully! Ready for fresh data.")

if __name__ == "__main__":
    clear_all_data()