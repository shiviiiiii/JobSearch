import sys
import os
import subprocess
import json
import time
import fitz  # PyMuPDF
import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from sentence_transformers import SentenceTransformer, util
from jobspy import scrape_jobs
from geopy.geocoders import Nominatim
from geopy.distance import geodesic

# --- PRE-FLIGHT CHECK ---
try:
    from jobspy import scrape_jobs
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "python-jobspy"])

# --- 1. SETUP & AUTH ---
def get_google_client():
    secret_json = os.getenv('GOOGLE_SHEET_CREDENTIALS')
    creds_dict = json.loads(secret_json)
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))

print("🤖 Waking up AI...")
model = SentenceTransformer('all-MiniLM-L6-v2')

def calculate_match(resume_text, job_blob):
    embeddings = model.encode([resume_text, job_blob], convert_to_tensor=True)
    return round(float(util.cos_sim(embeddings[0], embeddings[1])) * 100, 2)

# --- 2. ADZUNA API FETCH ---
def fetch_adzuna():
    print("🛰️ Fetching Adzuna...")
    APP_ID = os.getenv('ADZUNA_APP_ID')
    APP_KEY = os.getenv('ADZUNA_APP_KEY')
    if not APP_ID or not APP_KEY:
        print("⚠️ Adzuna Keys Missing. Skipping...")
        return []

    url = f"https://api.adzuna.com/v1/api/jobs/gb/search/1"
    params = {
        'app_id': APP_ID,
        'app_key': APP_KEY,
        'results_per_page': 50,
        'what': 'data analyst',
        'where': 'United Kingdom',
        'content-type': 'application/json'
    }
    
    try:
        r = requests.get(url, params=params)
        data = r.json().get('results', [])
        return [{
            'title': j.get('title'),
            'company': j.get('company', {}).get('display_name'),
            'location': j.get('location', {}).get('display_name'),
            'description': j.get('description'),
            'url': j.get('redirect_url'),
            'source': 'Adzuna'
        } for j in data]
    except Exception as e:
        print(f"Adzuna Error: {e}")
        return []

# --- 3. LINKEDIN SCRAPE ---
def fetch_linkedin():
    print("🛰️ Fetching LinkedIn...")
    try:
        jobs = scrape_jobs(
            site_name=["linkedin"],
            search_term='data analyst',
            location="United Kingdom",
            results_wanted=30,
            hours_old=72,
            linkedin_fetch_description=True
        )
        return [{
            'title': j['title'],
            'company': j['company'],
            'location': j['location'],
            'description': j['description'],
            'url': j['job_url'],
            'source': 'LinkedIn'
        } for _, j in jobs.iterrows()] if not jobs.empty else []
    except: return []

# --- 4. MAIN ---
def main():
    # 1. Load Resume
    with fitz.open("resume.pdf") as doc:
        resume_text = "".join([p.get_text() for p in doc])

    # 2. Gather All
    all_jobs = fetch_adzuna() + fetch_linkedin()
    if not all_jobs:
        print("No jobs found."); return

    # 3. Process
    geolocator = Nominatim(user_agent="norwich_job_bot_2026")
    norwich_coords = (52.6289, 1.2933)
    final_rows = []

    print(f"Filtering {len(all_jobs)} jobs...")
    for j in all_jobs:
        score = calculate_match(resume_text, f"{j['title']} {j['description']}")
        
        # Simple distance check
        dist_str = "N/A"
        try:
            city = j['location'].split(',')[0]
            loc = geolocator.geocode(f"{city}, UK", timeout=3)
            if loc:
                dist_str = f"{round(geodesic(norwich_coords, (loc.latitude, loc.longitude)).miles, 1)} miles"
            time.sleep(1)
        except: pass

        final_rows.append([score, j['title'], j['company'], j['location'], dist_str, j['url'], j['source']])

    # 4. Sort & Upload
    final_rows.sort(key=lambda x: x[0], reverse=True)
    gc = get_google_client()
    sh = gc.open('JobTracker_2026').sheet1
    sh.append_rows(final_rows)
    print(f"✅ Success! Sent {len(final_rows)} jobs to Google Sheets.")

if __name__ == "__main__":
    main()
