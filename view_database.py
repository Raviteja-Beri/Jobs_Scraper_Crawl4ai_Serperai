#!/usr/bin/env python3
"""
Database Viewer - Commands to view entire job database content
"""
import sqlite3
import json
from job_database import JobDatabase

def view_all_jobs():
    """View all jobs in the database"""
    db = JobDatabase()
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    
    print("=== ALL JOBS IN DATABASE ===")
    cursor.execute('''
        SELECT id, job_id, company, title, location, country, 
               extraction_date, complete_profile 
        FROM jobs 
        ORDER BY country, company, title
    ''')
    
    jobs = cursor.fetchall()
    for job in jobs:
        print(f"ID: {job[0]} | Job ID: {job[1]} | Company: {job[2]}")
        print(f"Title: {job[3]} | Location: {job[4]} | Country: {job[5]}")
        print(f"Date: {job[6]}")
        
        # Show complete profile if available
        if job[7]:
            try:
                profile = json.loads(job[7])
                print(f"Skills: {profile.get('skills', 'N/A')}")
                print(f"Apply URL: {profile.get('apply_url', 'N/A')}")
            except:
                print("Profile: Invalid JSON")
        print("-" * 80)
    
    conn.close()
    return len(jobs)

def view_jobs_by_country(country):
    """View jobs for specific country"""
    db = JobDatabase()
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    
    print(f"=== JOBS FOR {country.upper()} ===")
    cursor.execute('''
        SELECT id, job_id, company, title, location, 
               extraction_date, complete_profile 
        FROM jobs 
        WHERE country = ?
        ORDER BY company, title
    ''', (country,))
    
    jobs = cursor.fetchall()
    for job in jobs:
        print(f"ID: {job[0]} | Job ID: {job[1]} | Company: {job[2]}")
        print(f"Title: {job[3]} | Location: {job[4]}")
        print(f"Date: {job[5]}")
        
        # Show complete profile if available
        if job[6]:
            try:
                profile = json.loads(job[6])
                print(f"Skills: {profile.get('skills', 'N/A')}")
                print(f"Apply URL: {profile.get('apply_url', 'N/A')}")
            except:
                print("Profile: Invalid JSON")
        print("-" * 60)
    
    conn.close()
    return len(jobs)

def export_to_json(filename="database_export.json"):
    """Export entire database to JSON"""
    db = JobDatabase()
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, job_id, company, title, location, experience, 
               job_type, description, skills, apply_link, country, 
               extraction_date, complete_profile, created_at
        FROM jobs 
        ORDER BY country, company
    ''')
    
    jobs = []
    for row in cursor.fetchall():
        job = {
            "id": row[0],
            "job_id": row[1],
            "company": row[2],
            "title": row[3],
            "location": row[4],
            "experience": row[5],
            "job_type": row[6],
            "description": row[7],
            "skills": row[8],
            "apply_link": row[9],
            "country": row[10],
            "extraction_date": row[11],
            "complete_profile": json.loads(row[12]) if row[12] else None,
            "created_at": row[13]
        }
        jobs.append(job)
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)
    
    conn.close()
    print(f"Exported {len(jobs)} jobs to {filename}")
    return len(jobs)

def get_database_stats():
    """Get comprehensive database statistics"""
    db = JobDatabase()
    stats = db.get_stats()
    
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    
    print("=== DATABASE STATISTICS ===")
    print(f"Total Jobs: {stats['total_jobs']}")
    print(f"Total Companies: {stats['total_companies']}")
    print("\nJobs by Country:")
    for country, count in stats['jobs_by_country'].items():
        print(f"  {country}: {count} jobs")
    
    # Get companies info
    print("\n=== COMPANIES ===")
    cursor.execute('SELECT name, website, country, total_jobs, last_scraped FROM companies ORDER BY country, name')
    companies = cursor.fetchall()
    for company in companies:
        print(f"Company: {company[0]} | Website: {company[1]} | Country: {company[2]} | Jobs: {company[3]} | Last Scraped: {company[4]}")
    
    conn.close()

if __name__ == "__main__":
    print("DATABASE VIEWER COMMANDS")
    print("=" * 50)
    
    # Show stats first
    get_database_stats()
    
    print("\n" + "=" * 50)
    print("AVAILABLE COMMANDS:")
    print("1. view_all_jobs() - View all jobs")
    print("2. view_jobs_by_country('USA') - View USA jobs")
    print("3. view_jobs_by_country('Germany') - View Germany jobs") 
    print("4. view_jobs_by_country('India') - View India jobs")
    print("5. export_to_json('all_jobs.json') - Export to JSON")
    print("6. get_database_stats() - Show statistics")
    
    print("\nTo run any command, use:")
    print("python view_database.py")
    print("Then in Python shell:")
    print("from view_database import *")
    print("view_all_jobs()")