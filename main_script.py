import os
import sys
import subprocess

# --- THE EXPERT FIX ---
try:
    from jobspy import scrape_jobs
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "python-jobspy"])
    from jobspy import scrape_jobs

import json
import time
import fitz  # PyMuPDF
import gspread
import requests
from google.oauth2.service_account import Credentials
from sentence_transformers import SentenceTransformer, util
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from datetime import datetime

# --- 1. SETUP ---
def get_google_client():
    secret_json = os.getenv('GOOGLE_SHEET_CREDENTIALS')
    creds_dict = json.loads(secret_json)
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, 
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))

print("🤖 Loading AI Model...")
model = SentenceTransformer('all-MiniLM-L6-v2')

def calculate_match(resume_text, job_blob):
    embeddings = model.encode([resume_text, job_blob], convert_to_tensor=True)
    return round(float(util.cos_sim(embeddings[0], embeddings[1])) * 100, 2)

def classify_job(title, desc):
    text = (str(title) + " " + str(desc)).lower()
    # Seniority
    if any(x in text for x in ['senior', 'lead', 'principal', 'sr.', 'manager']):
        level = "Senior"
    elif any(x in text for x in ['junior', 'entry', 'graduate', 'intern', 'trainee', 'associate']):
        level = "Entry/Junior"
    else:
        level = "Mid"
    # Environment
    if any(x in text for x in ['remote', 'work from home', 'wfh']):
        env = "Remote"
    elif any(x in text for x in ['hybrid', 'flexible working']):
        env = "Hybrid"
    else:
        env = "Onsite"
    return level, env

# SHORTER LIST FOR SPEED
SEARCH_TERMS = ['Data Analyst', 'Data Engineer', 'Business Intelligence']

# --- 2. FETCHERS ---
def fetch_adzuna():
    APP_ID, APP_KEY = os.getenv('ADZUNA_APP_ID'), os.getenv('ADZUNA_APP_KEY')
    if not APP_ID or not APP_KEY: return []
    
    results = []
    for term in SEARCH_TERMS:
        url = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
        # Reduced to 20 results per term for speed
        params = {'app_id': APP_ID, 'app_key': APP_KEY, 'results_per_page': 20, 'what': term, 'where': 'UK'}
        try:
            r = requests.get(url, params=params, timeout=5).json().get('results', [])
            for j in r:
                level, env = classify_job(j.get('title', ''), j.get('description', ''))
                results.append({
                    'title': j.get('title'), 'company': j.get('company', {}).get('display_name'),
                    'location': j.get('location', {}).get('display_name'), 'desc': j.get('description'),
                    'url': j.get('redirect_url'), 'source': 'Adzuna',
                    'env': env, 'level': level, 'deadline': 'NIL'
                })
        except: continue
    return results

def fetch_linkedin():
    results = []
    for term in SEARCH_TERMS:
        try:
            # results_wanted=25 is the "sweet spot" for speed vs quantity
            df = scrape_jobs(site_name=["linkedin"], search_term=term, location="United Kingdom", 
                             results_wanted=25, hours_old=72, linkedin_fetch_description=True)
            for _, row in df.iterrows():
                level, env = classify_job(row.get('title', ''), row.get('description', ''))
                results.append({
                    'title': row['title'], 'company': row['company'], 'location': row['location'],
                    'desc': row['description'], 'job_url': row['job_url'], 'source': 'LinkedIn',
                    'env': env, 'level': level
                })
            time.sleep(2) # Smaller sleep to save time
        except: continue
    return results

# --- 3. MAIN ENGINE ---
# --- 3. MAIN ENGINE ---
def main():
    print("📄 Reading Resume...")
    with fitz.open("resume.pdf") as doc:
        resume_text = "".join([p.get_text() for p in doc])

    client = get_google_client()
    sheet = client.open('JobSearch').sheet1 # Ensure this matches your Sheet name
    
    existing = sheet.get_all_values()
    # Headers updated: "Distance" is gone
    if not existing:
        headers = ["Score", "Title", "Company", "Location", "Date Found", "Link", "Source", "Environment", "Seniority", "Deadline"]
        sheet.append_row(headers)
        existing_urls = set()
    else:
        # Link is now at index 5 because Distance was removed
        existing_urls = set([row[5] for row in existing if len(row) > 5])

    print("🔍 Fetching Jobs...")
    # Using your fetchers
    all_jobs = fetch_adzuna() + fetch_linkedin()
    
    today = datetime.now().strftime('%Y-%m-%d')
    upload_batch = []
    seen_urls = set()

    print(f"🧠 Scoring {len(all_jobs)} jobs...")
    for j in all_jobs:
        url = j.get('url') or j.get('job_url')
        if url in existing_urls or url in seen_urls: continue
        seen_urls.add(url)

        # AI Scoring
        score = calculate_match(resume_text, f"{j['title']} {j['desc']}")
        
        # Mapping to 10 columns (Distance removed)
        upload_batch.append([
            score, 
            j['title'], 
            j['company'], 
            j['location'], 
            today, 
            url, 
            j['source'], 
            j['env'], 
            j['level'], 
            'NIL'
        ])

    if upload_batch:
        upload_batch.sort(key=lambda x: x[0], reverse=True)
        sheet.append_rows(upload_batch)
        print(f"🚀 Success! {len(upload_batch)} jobs added to Google Sheets.")
    else:
        print("Check: No new unique jobs found this time.")

if __name__ == "__main__":
    main()
