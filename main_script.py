import os
import sys
import subprocess
import json
import time
import gspread
import requests
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- THE EXPERT FIX ---
try:
    from jobspy import scrape_jobs
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "python-jobspy"])
    from jobspy import scrape_jobs

# --- 1. SETUP (AI MODEL REMOVED FOR SPEED) ---
def get_google_client():
    secret_json = os.getenv('GOOGLE_SHEET_CREDENTIALS')
    creds_dict = json.loads(secret_json)
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, 
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))

# EXPERT CLASSIFICATION ENGINE (Kept because it's fast text matching)
def classify_job(title, desc):
    text = (str(title) + " " + str(desc)).lower()
    
    if any(x in text for x in ['senior', 'lead', 'principal', 'sr.', 'manager']):
        level = "Senior"
    elif any(x in text for x in ['junior', 'entry', 'graduate', 'intern', 'trainee']):
        level = "Entry/Junior"
    else:
        level = "Mid"
    
    if any(x in text for x in ['remote', 'work from home', 'wfh']):
        env = "Remote"
    elif any(x in text for x in ['hybrid', 'flexible working']):
        env = "Hybrid"
    else:
        env = "Onsite"
    
    return level, env

# Use 'data' here as requested
SEARCH_TERMS = ['data analyst', 'data engineer', 'data']

# --- 2. FETCHERS ---
def fetch_adzuna():
    APP_ID, APP_KEY = os.getenv('ADZUNA_APP_ID'), os.getenv('ADZUNA_APP_KEY')
    if not APP_ID or not APP_KEY: return []
    results = []
    for term in SEARCH_TERMS:
        url = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
        params = {'app_id': APP_ID, 'app_key': APP_KEY, 'results_per_page': 30, 'what': term, 'where': 'UK'}
        try:
            r = requests.get(url, params=params, timeout=10).json().get('results', [])
            for j in r:
                level, env = classify_job(j.get('title', ''), j.get('description', ''))
                results.append({
                    'title': j.get('title'), 'company': j.get('company', {}).get('display_name'),
                    'location': j.get('location', {}).get('display_name'), 'desc': j.get('description'),
                    'url': j.get('redirect_url'), 'source': 'Adzuna', 'env': env, 'level': level
                })
        except: continue
    return results

def fetch_linkedin():
    results = []
    for term in SEARCH_TERMS:
        try:
            # We skip 'linkedin_fetch_description=True' to save massive amounts of time
            df = scrape_jobs(site_name=["linkedin"], search_term=term, location="United Kingdom", 
                             results_wanted=30, hours_old=48)
            for _, row in df.iterrows():
                level, env = classify_job(row.get('title', ''), "")
                results.append({
                    'title': row['title'], 'company': row['company'], 'location': row['location'],
                    'url': row['job_url'], 'source': 'LinkedIn', 'env': env, 'level': level
                })
        except: continue
    return results

# --- 3. MAIN ENGINE (SPEED OPTIMIZED) ---
def main():
    print("🚀 Running in High-Speed Mode (No AI Scoring)")
    
    client = get_google_client()
    sheet = client.open('JobSearch').sheet1
    
    existing_data = sheet.get_all_values()
    
    if not existing_data:
        headers = ["Title", "Company", "Location", "Date Found", "Link", "Source", "Environment", "Seniority", "Deadline"]
        sheet.append_row(headers)
        existing_urls = set()
    else:
        # Link is in the 5th column (index 4)
        existing_urls = set([row[4] for row in existing_data if len(row) > 4])

    print("🔍 Fetching...")
    all_jobs = fetch_adzuna() + fetch_linkedin()
    
    today = datetime.now().strftime('%Y-%m-%d')
    upload_batch = []
    seen_urls = set()

    for j in all_jobs:
        url = j.get('url') or j.get('job_url')
        if url in existing_urls or url in seen_urls: continue
        seen_urls.add(url)

        upload_batch.append([
            j['title'], j['company'], j['location'], 
            today, url, j['source'], j['env'], j['level'], 'NIL'
        ])

    if upload_batch:
        sheet.append_rows(upload_batch)
        print(f"✅ Success! {len(upload_batch)} new jobs added.")
    else:
        print("Done. No new jobs found today.")

if __name__ == "__main__":
    main()
