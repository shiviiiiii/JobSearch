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
from datetime import datetime

# --- 1. SETUP ---
def get_google_client():
    secret_json = os.getenv('GOOGLE_SHEET_CREDENTIALS')
    creds_dict = json.loads(secret_json)
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))

print("🤖 Loading AI...")
model = SentenceTransformer('all-MiniLM-L6-v2')

def calculate_match(resume_text, job_blob):
    embeddings = model.encode([resume_text, job_blob], convert_to_tensor=True)
    return round(float(util.cos_sim(embeddings[0], embeddings[1])) * 100, 2)

# --- 2. ADZUNA (Targeting 50) ---
def fetch_adzuna():
    APP_ID = os.getenv('ADZUNA_APP_ID')
    APP_KEY = os.getenv('ADZUNA_APP_KEY')
    print(f"🛰️ Adzuna: Requesting 50 jobs...")
    
    # We request page 1 with 50 results
    url = f"https://api.adzuna.com/v1/api/jobs/gb/search/1"
    params = {
        'app_id': APP_ID,
        'app_key': APP_KEY,
        'results_per_page': 50,
        'what': 'data analyst',
        'where': 'UK',
        'content-type': 'application/json'
    }
    
    try:
        r = requests.get(url, params=params, timeout=15)
        results = r.json().get('results', [])
        return [{
            'title': j.get('title'),
            'company': j.get('company', {}).get('display_name'),
            'location': j.get('location', {}).get('display_name'),
            'description': j.get('description'),
            'url': j.get('redirect_url'),
            'source': 'Adzuna'
        } for j in results]
    except Exception as e:
        print(f"❌ Adzuna Error: {e}")
        return []

# --- 3. LINKEDIN (Targeting 50) ---
def fetch_linkedin():
    print("🛰️ LinkedIn: Requesting 50 jobs...")
    try:
        # results_wanted=50 directly
        df = scrape_jobs(
            site_name=["linkedin"],
            search_term='data analyst',
            location="United Kingdom",
            results_wanted=50, 
            hours_old=72,
            linkedin_fetch_description=True
        )
        return [{
            'title': row['title'],
            'company': row['company'],
            'location': row['location'],
            'description': row['description'],
            'url': row['job_url'],
            'source': 'LinkedIn'
        } for _, row in df.iterrows()] if not df.empty else []
    except Exception as e:
        print(f"❌ LinkedIn Error: {e}")
        return []

# --- 4. MAIN ---
def main():
    with fitz.open("resume.pdf") as doc:
        resume_text = "".join([p.get_text() for p in doc])

    # Fetch both
    adzuna_list = fetch_adzuna()
    linkedin_list = fetch_linkedin()
    
    print(f"📊 Raw Totals: Adzuna({len(adzuna_list)}) | LinkedIn({len(linkedin_list)})")
    
    all_jobs = adzuna_list + linkedin_list
    if not all_jobs:
        print("📭 No jobs found."); return

    geolocator = Nominatim(user_agent="norwich_job_bot_2026")
    norwich_coords = (52.6289, 1.2933)
    today = datetime.now().strftime('%Y-%m-%d')
    
    final_rows = []
    seen_urls = set()

    for j in all_jobs:
        if j['url'] in seen_urls: continue
        seen_urls.add(j['url'])

        score = calculate_match(resume_text, f"{j['title']} {j['description']}")
        
        # Distance Logic
        dist_str = "N/A"
        try:
            city = str(j['location']).split(',')[0].replace("Area", "").strip()
            loc = geolocator.geocode(f"{city}, UK", timeout=2)
            if loc:
                dist_str = f"{round(geodesic(norwich_coords, (loc.latitude, loc.longitude)).miles, 1)} miles"
            time.sleep(1)
        except: pass

        # Columns: [Score, Title, Company, Location, Distance, Date, Link, Source]
        final_rows.append([score, j['title'], j['company'], j['location'], dist_str, today, j['url'], j['source']])

    # Sort
    final_rows.sort(key=lambda x: x[0], reverse=True)

    # Upload
    client = get_google_client()
    sheet = client.open('JobTracker_2026').sheet1
    sheet.append_rows(final_rows)
    
    # Final count for your logs
    adz_count = sum(1 for row in final_rows if row[7] == 'Adzuna')
    li_count = sum(1 for row in final_rows if row[7] == 'LinkedIn')
    print(f"🚀 SUCCESS! Added {len(final_rows)} total jobs.")
    print(f"📈 Final Breakdown: {adz_count} Adzuna | {li_count} LinkedIn")

if __name__ == "__main__":
    main()
