#!/usr/bin/env python3
"""
Export entire database to JSON
"""
import sqlite3
import json
from job_database import JobDatabase

def export_all_to_json():
    """Export entire database to JSON file"""
    db = JobDatabase()
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    
    # Get all jobs with complete data
    cursor.execute('''
        SELECT id, job_id, company, title, location, experience, 
               job_type, description, skills, apply_link, country, 
               extraction_date, complete_profile, created_at
        FROM jobs 
        ORDER BY country, company, title
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
            "created_at": row[13]
        }
        
        # Add complete profile if available
        if row[12]:
            try:
                job["complete_profile"] = json.loads(row[12])
            except:
                job["complete_profile"] = None
        else:
            job["complete_profile"] = None
            
        jobs.append(job)
    
    # Export to JSON file
    filename = "all_109_jobs_complete.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)
    
    conn.close()
    print(f"✅ Exported {len(jobs)} jobs to {filename}")
    return filename

if __name__ == "__main__":
    export_all_to_json()