
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

# EXPERT CLASSIFICATION ENGINE
def classify_job(title, desc):
    text = (str(title) + " " + str(desc)).lower()
    
    # 1. Seniority Classification (Entry/Junior, Mid, Senior)
    if any(x in text for x in ['senior', 'lead', 'principal', 'head of', 'sr.', 'manager', 'iii', 'iv']):
        level = "Senior"
    elif any(x in text for x in ['junior', 'entry', 'graduate', 'intern', 'trainee', 'associate', 'i', 'ii', 'apprentice']):
        level = "Entry/Junior"
    else:
        level = "Mid"
    
    # 2. Work Environment (Remote, Hybrid, Onsite)
    if any(x in text for x in ['remote', 'work from home', 'wfh', 'anywhere']):
        env = "Remote"
    elif any(x in text for x in ['hybrid', 'flexible working', 'split office', '2 days office']):
        env = "Hybrid"
    else:
        env = "Onsite"
    
    return level, env

SEARCH_TERMS = ['data analyst', 'junior data analyst', 'data engineer', 'business intelligence', 'reporting analyst', 'data']

# --- 2. FETCHERS ---
def fetch_adzuna():
    APP_ID, APP_KEY = os.getenv('ADZUNA_APP_ID'), os.getenv('ADZUNA_APP_KEY')
    if not APP_ID or not APP_KEY: return []
    
    results = []
    for term in SEARCH_TERMS:
        url = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
        params = {'app_id': APP_ID, 'app_key': APP_KEY, 'results_per_page': 50, 'what': term, 'where': 'UK'}
        try:
            r = requests.get(url, params=params, timeout=10).json().get('results', [])
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
            df = scrape_jobs(site_name=["linkedin"], search_term=term, location="United Kingdom", 
                             results_wanted=50, hours_old=72, linkedin_fetch_description=True)
            for _, row in df.iterrows():
                level, env = classify_job(row.get('title', ''), row.get('description', ''))
                results.append({
                    'title': row['title'], 'company': row['company'], 'location': row['location'],
                    'desc': row['description'], 'url': row['job_url'], 'source': 'LinkedIn',
                    'env': env, 'level': level, 'deadline': 'NIL'
                })
            time.sleep(5)
        except: continue
    return results

# --- 3. MAIN ENGINE ---
def main():
    with fitz.open("resume.pdf") as doc:
        resume_text = "".join([p.get_text() for p in doc])

    client = get_google_client()
    sheet = client.open('JobTracker_2026').sheet1
    
    existing = sheet.get_all_values()
    if not existing:
        headers = ["Score", "Title", "Company", "Location", "Distance", "Date Found", "Link", "Source", "Environment", "Seniority", "Deadline"]
        sheet.append_row(headers)
        existing_urls = set()
    else:
        existing_urls = set([row[6] for row in existing if len(row) > 6])

    all_jobs = fetch_adzuna() + fetch_linkedin()
    geolocator = Nominatim(user_agent="norwich_job_bot_2026")
    home_coords = (52.6289, 1.2933)
    today = datetime.now().strftime('%Y-%m-%d')
    
    upload_batch = []
    seen_urls = set()

    for j in all_jobs:
        if j['url'] in existing_urls or j['url'] in seen_urls: continue
        seen_urls.add(j['url'])

        score = calculate_match(resume_text, f"{j['title']} {j['desc']}")
        
        dist = "Unknown"
        try:
            loc = geolocator.geocode(f"{str(j['location']).split(',')[0]}, UK", timeout=3)
            if loc:
                dist = f"{round(geodesic(home_coords, (loc.latitude, loc.longitude)).miles, 1)} miles"
        except: pass

        # FINAL MAPPING TO 11 COLUMNS
        upload_batch.append([
            score, j['title'], j['company'], j['location'], 
            dist, today, j['url'], j['source'], 
            j['env'], j['level'], j['deadline']
        ])

    if upload_batch:
        upload_batch.sort(key=lambda x: x[0], reverse=True)
        sheet.append_rows(upload_batch)
        print(f"🚀 Success! Found {len(upload_batch)} new jobs with Seniority & Environment tags.")

if __name__ == "__main__":
    main()
