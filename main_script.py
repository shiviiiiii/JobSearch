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

print("🤖 Loading AI Model...")
model = SentenceTransformer('all-MiniLM-L6-v2')

def calculate_match(resume_text, job_blob):
    embeddings = model.encode([resume_text, job_blob], convert_to_tensor=True)
    return round(float(util.cos_sim(embeddings[0], embeddings[1])) * 100, 2)

# EXPANDED SEARCH: Includes Engineer, Assistant, Support, and Lead roles
SEARCH_TERMS = [
    'data analyst', 'junior data analyst', 'data engineer', 
    'data assistant', 'data support', 'business intelligence',
    'insight analyst', 'reporting analyst', 'data manager'
]

# --- 2. FETCHERS ---
def fetch_adzuna():
    APP_ID = os.getenv('ADZUNA_APP_ID')
    APP_KEY = os.getenv('ADZUNA_APP_KEY')
    
    if not APP_ID or not APP_KEY:
        print("❌ ADZUNA ERROR: App ID or Key is missing from GitHub Secrets!")
        return []
    
    all_adzuna = []
    for term in SEARCH_TERMS:
        print(f"🛰️ Adzuna: Searching '{term}'...")
        # Adzuna API v1 URL
        url = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
        params = {
            'app_id': APP_ID, 
            'app_key': APP_KEY, 
            'results_per_page': 20, 
            'what': term, 
            'where': 'UK',
            'content-type': 'application/json'
        }
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code != 200:
                print(f"⚠️ Adzuna API returned status {r.status_code}")
                continue
            
            data = r.json().get('results', [])
            print(f"🔍 Found {len(data)} Adzuna results for '{term}'")
            
            for j in data:
                all_adzuna.append({
                    'title': j.get('title'),
                    'company': j.get('company', {}).get('display_name'),
                    'location': j.get('location', {}).get('display_name'),
                    'description': j.get('description'),
                    'url': j.get('redirect_url'),
                    'source': 'Adzuna'
                })
            time.sleep(1) # Rate limiting protection
        except Exception as e:
            print(f"❌ Adzuna Request Failed: {e}")
    return all_adzuna

def fetch_linkedin():
    all_li = []
    for term in SEARCH_TERMS:
        print(f"🛰️ LinkedIn: Scraping '{term}'...")
        try:
            # results_wanted=20 per term to avoid getting blocked
            df = scrape_jobs(
                site_name=["linkedin"],
                search_term=term,
                location="United Kingdom",
                results_wanted=20, 
                hours_old=72,
                linkedin_fetch_description=True
            )
            for _, row in df.iterrows():
                all_li.append({
                    'title': row['title'], 
                    'company': row['company'], 
                    'location': row['location'],
                    'description': row['description'], 
                    'url': row['job_url'], 
                    'source': 'LinkedIn'
                })
            time.sleep(5)
        except: continue
    return all_li

# --- 3. MAIN ---
def main():
    with fitz.open("resume.pdf") as doc:
        resume_text = "".join([p.get_text() for p in doc])

    client = get_google_client()
    sheet = client.open('JobTracker_2026').sheet1
    
    existing_records = sheet.get_all_values()
    existing_urls = set([row[6] for row in existing_records if len(row) > 6])

    # Fetch from both sources
    adz_data = fetch_adzuna()
    li_data = fetch_linkedin()
    
    print(f"📊 SUMMARY: Adzuna found {len(adz_data)} | LinkedIn found {len(li_data)}")
    
    all_found = adz_data + li_data
    
    geolocator = Nominatim(user_agent="norwich_job_bot_2026")
    norwich_coords = (52.6289, 1.2933)
    today = datetime.now().strftime('%Y-%m-%d')
    
    new_rows = []
    local_seen_urls = set()

    for j in all_found:
        url = j['url']
        if url in existing_urls or url in local_seen_urls:
            continue
        local_seen_urls.add(url)

        # NO FILTERING: All seniority levels allowed
        score = calculate_match(resume_text, f"{j['title']} {j['description']}")
        
        dist_str = "N/A"
        try:
            city = str(j['location']).split(',')[0].replace("Area", "").strip()
            loc = geolocator.geocode(f"{city}, UK", timeout=2)
            if loc:
                dist_str = f"{round(geodesic(norwich_coords, (loc.latitude, loc.longitude)).miles, 1)} miles"
            time.sleep(1)
        except: pass

        new_rows.append([score, j['title'], j['company'], j['location'], dist_str, today, url, j['source']])

    if new_rows:
        new_rows.sort(key=lambda x: x[0], reverse=True)
        sheet.append_rows(new_rows)
        print(f"🚀 Success! Added {len(new_rows)} NEW jobs (including High Level).")
    else:
        print("📭 No new jobs found this time.")

if __name__ == "__main__":
    main()
