import os
import sys
import subprocess
import json
import time
import gspread
import requests
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- AUTOMATIC INSTALL FOR GITHUB ACTIONS ---
try:
    from jobspy import scrape_jobs
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "python-jobspy"])
    from jobspy import scrape_jobs

# --- 1. SETUP ---
def get_google_client():
    secret_json = os.getenv('GOOGLE_SHEET_CREDENTIALS')
    if not secret_json:
        raise ValueError("GOOGLE_SHEET_CREDENTIALS secret is not set!")
    creds_dict = json.loads(secret_json)
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, 
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))

def classify_job(title, desc):
    text = (str(title) + " " + str(desc)).lower()
    
    # Seniority Classification
    if any(x in text for x in ['senior', 'lead', 'principal', 'sr.', 'manager', 'head']):
        level = "Senior"
    elif any(x in text for x in ['junior', 'entry', 'graduate', 'intern', 'trainee', 'apprentice']):
        level = "Entry/Junior"
    else:
        level = "Mid"
    
    # Environment Classification
    if any(x in text for x in ['remote', 'work from home', 'wfh', 'anywhere']):
        env = "Remote"
    elif any(x in text for x in ['hybrid', 'flexible working', 'partially remote']):
        env = "Hybrid"
    else:
        env = "Onsite"
    
    return level, env

SEARCH_TERMS = ['data analyst', 'data engineer']

# --- 2. FETCHERS ---
def fetch_adzuna():
    APP_ID, APP_KEY = os.getenv('ADZUNA_APP_ID'), os.getenv('ADZUNA_APP_KEY')
    if not APP_ID or not APP_KEY: 
        print("⚠️ Adzuna credentials missing. Skipping...")
        return []
    
    results = []
    for term in SEARCH_TERMS:
        url = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
        params = {
            'app_id': APP_ID, 
            'app_key': APP_KEY, 
            'results_per_page': 30, 
            'what': term, 
            'where': 'UK',
            'content-type': 'application/json'
        }
        try:
            r = requests.get(url, params=params, timeout=15).json().get('results', [])
            for j in r:
                level, env = classify_job(j.get('title', ''), j.get('description', ''))
                results.append({
                    'title': j.get('title'),
                    'company': j.get('company', {}).get('display_name'),
                    'location': j.get('location', {}).get('display_name'),
                    'url': j.get('redirect_url'),
                    'source': 'Adzuna',
                    'env': env,
                    'level': level
                })
        except Exception as e:
            print(f"❌ Adzuna error for {term}: {e}")
            continue
    return results

def fetch_linkedin():
    results = []
    for term in SEARCH_TERMS:
        try:
            # We use a smaller results_wanted for speed and reliability
            df = scrape_jobs(
                site_name=["linkedin"], 
                search_term=term, 
                location="United Kingdom", 
                results_wanted=15, 
                hours_old=48,
                linkedin_fetch_description=False 
            )
            
            # THE FIX: Iterate through the DataFrame and build the dict list
            if not df.empty:
                for _, row in df.iterrows():
                    # Since description is False, we classify based on Title
                    level, env = classify_job(row.get('title', ''), "")
                    results.append({
                        'title': row.get('title'),
                        'company': row.get('company'),
                        'location': row.get('location'),
                        'url': row.get('job_url'),
                        'source': 'LinkedIn',
                        'env': env,
                        'level': level
                    })
        except Exception as e:
            print(f"❌ LinkedIn error for {term}: {e}")
            continue
    return results

# --- 3. MAIN ENGINE ---
def main():
    print(f"🚀 Job Search Automation started at {datetime.now().strftime('%H:%M:%S')}")
    
    try:
        client = get_google_client()
        sheet = client.open('JobSearch').sheet1
    except Exception as e:
        print(f"❌ Could not connect to Google Sheets: {e}")
        return

    # Fetch existing data to prevent duplicates
    existing_rows = sheet.get_all_values()
    
    if not existing_rows:
        headers = ["Title", "Company", "Location", "Date Found", "Link", "Source", "Environment", "Seniority", "Deadline"]
        sheet.append_row(headers)
        existing_urls = set()
    else:
        # We store URLs in a set for O(1) lookup speed. URL is at index 4.
        existing_urls = {str(row[4]).strip() for row in existing_rows if len(row) > 4}

    print("🔍 Scraping fresh leads...")
    all_jobs = fetch_adzuna() + fetch_linkedin()
    
    today = datetime.now().strftime('%Y-%m-%d')
    upload_batch = []
    seen_in_current_run = set()

    for j in all_jobs:
        raw_url = j.get('url')
        if not raw_url: continue
        
        url = str(raw_url).strip()
        
        # Check against Sheet and current run list
        if url in existing_urls or url in seen_in_current_run:
            continue
            
        seen_in_current_run.add(url)
        upload_batch.append([
            j.get('title', 'N/A'), 
            j.get('company', 'N/A'), 
            j.get('location', 'N/A'), 
            today, 
            url, 
            j.get('source', 'N/A'), 
            j.get('env', 'Onsite'), 
            j.get('level', 'Mid'), 
            'NIL'
        ])

    if upload_batch:
        sheet.append_rows(upload_batch)
        print(f"✅ Success! {len(upload_batch)} new unique jobs added.")
    else:
        print("✨ No new unique jobs found this time.")

if __name__ == "__main__":
    main()
##bla bla bla ##
print("Hello world")