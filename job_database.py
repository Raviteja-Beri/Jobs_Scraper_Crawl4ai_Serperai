"""
SQLite Database Handler for Job Storage
"""
import sqlite3
import json
from datetime import datetime
from typing import List, Dict

class JobDatabase:
    def __init__(self, db_path: str = "jobs.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create jobs table with enhanced schema for complete profiles
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                company TEXT NOT NULL,
                title TEXT NOT NULL,
                location TEXT,
                experience TEXT,
                job_type TEXT,
                description TEXT,
                skills TEXT,
                apply_link TEXT,
                country TEXT,
                extraction_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(job_id, company) ON CONFLICT REPLACE
            )
        ''')
        
        # Create companies table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                website TEXT,
                country TEXT,
                total_jobs INTEGER DEFAULT 0,
                last_scraped TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def save_jobs(self, jobs: List[Dict], country: str) -> int:
        """Save jobs to database with complete profile support"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        extraction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        saved_count = 0
        updated_count = 0
        
        for job in jobs:
            try:
                job_id = job.get('job_id', '')
                
                # Check if job already exists by job_id and company
                cursor.execute('SELECT id FROM jobs WHERE job_id = ? AND company = ?', 
                             (job_id, job.get('company', '')))
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing job
                    cursor.execute('''
                        UPDATE jobs SET
                            title = ?, location = ?, experience = ?,
                            job_type = ?, description = ?, skills = ?, 
                            apply_link = ?, country = ?, extraction_date = ?,
                            created_at = CURRENT_TIMESTAMP
                        WHERE job_id = ? AND company = ?
                    ''', (
                        job.get('title', job.get('job_title', '')),
                        job.get('location', ''),
                        job.get('experience', job.get('employment_type', '')),
                        job.get('type', job.get('employment_type', '')),
                        job.get('description', job.get('job_description', '')),
                        job.get('skills', '') if isinstance(job.get('skills'), str) else ', '.join(job.get('skills', [])),
                        job.get('apply_link', job.get('apply_url', '')),
                        country,
                        extraction_date,
                        job_id,
                        job.get('company', '')
                    ))
                    updated_count += 1
                else:
                    # Insert new job
                    cursor.execute('''
                        INSERT INTO jobs (job_id, company, title, location, experience, job_type,
                                        description, skills, apply_link, country, extraction_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        job_id,
                        job.get('company', ''),
                        job.get('title', job.get('job_title', '')),
                        job.get('location', ''),
                        job.get('experience', job.get('employment_type', '')),
                        job.get('type', job.get('employment_type', '')),
                        job.get('description', job.get('job_description', '')),
                        job.get('skills', '') if isinstance(job.get('skills'), str) else ', '.join(job.get('skills', [])),
                        job.get('apply_link', job.get('apply_url', '')),
                        country,
                        extraction_date
                    ))
                    saved_count += 1
            except Exception as e:
                print(f"Error saving job: {e}")
        
        conn.commit()
        conn.close()
        
        if updated_count > 0:
            print(f"  Updated {updated_count} existing jobs")
        
        return saved_count
    
    def save_company(self, company_name: str, website: str, country: str, job_count: int):
        """Save or update company information"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        last_scraped = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Use INSERT OR REPLACE to update existing companies
        cursor.execute('''
            INSERT OR REPLACE INTO companies (name, website, country, total_jobs, last_scraped)
            VALUES (?, ?, ?, ?, ?)
        ''', (company_name, website, country, job_count, last_scraped))
        
        conn.commit()
        conn.close()
    
    def clear_all_jobs_for_country(self, country: str):
        """Clear ALL jobs for a country before adding new ones"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM jobs WHERE country = ?', (country,))
        deleted_count = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        if deleted_count > 0:
            print(f"Cleared {deleted_count} old jobs for {country}")
        return deleted_count
    
        return []
    
    def get_stats(self) -> Dict:
        """Get database statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total jobs
        cursor.execute('SELECT COUNT(*) FROM jobs')
        total_jobs = cursor.fetchone()[0]
        
        # Total companies
        cursor.execute('SELECT COUNT(*) FROM companies')
        total_companies = cursor.fetchone()[0]
        
        # Jobs by country
        cursor.execute('SELECT country, COUNT(*) FROM jobs GROUP BY country')
        jobs_by_country = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            'total_jobs': total_jobs,
            'total_companies': total_companies,
            'jobs_by_country': jobs_by_country
        }