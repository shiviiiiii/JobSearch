import sys
import os
import subprocess
import json
import time
import fitz  # PyMuPDF
import gspread
import requests
from google.oauth2.service_account import Credentials
from sentence_transformers import SentenceTransformer, util
from jobspy import scrape_jobs
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from datetime import datetime

# --- 1. AUTH & AI SETUP ---
def get_google_client():
    secret_json = os.getenv('GOOGLE_SHEET_CREDENTIALS')
    creds_dict = json.loads(secret_json)
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))

print("🤖 Loading AI Model...")
model = SentenceTransformer('all-MiniLM-L6-v2')

def calculate_match(resume_text, job_blob):
    embeddings = model.encode([resume_text, job_blob], convert_to_tensor=True)
    return round(float(util.cos_sim(embeddings[0], embeddings[1])) * 100, 2)

# --- 2. FETCHERS (ADZUNA + LINKEDIN) ---
def fetch_adzuna(terms):
    APP_ID = os.getenv('ADZUNA_APP_ID')
    APP_KEY = os.getenv('ADZUNA_APP_KEY')
    all_adz = []
    for term in terms:
        print(f"🛰️ Adzuna: Searching '{term}'...")
        url = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
        params = {'app_id': APP_ID, 'app_key': APP_KEY, 'results_per_page': 25, 'what': term, 'where': 'UK'}
        try:
            r = requests.get(url, params=params, timeout=10)
            for j in r.json().get('results', []):
                all_adz.append({
                    'title': j.get('title'), 'company': j.get('company', {}).get('display_name'),
                    'location': j.get('location', {}).get('display_name'), 'desc': j.get('description'),
                    'url': j.get('redirect_url'), 'source': 'Adzuna'
                })
        except: continue
    return all_adz

def fetch_linkedin(terms):
    all_li = []
    for term in terms:
        print(f"🛰️ LinkedIn: Scraping '{term}'...")
        try:
            df = scrape_jobs(site_name=["linkedin"], search_term=term, location="United Kingdom", results_wanted=15, hours_old=72, linkedin_fetch_description=True)
            for _, row in df.iterrows():
                all_li.append({
                    'title': row['title'], 'company': row['company'], 'location': row['location'],
                    'desc': row['description'], 'url': row['job_url'], 'source': 'LinkedIn'
                })
            time.sleep(5)
        except: continue
    return all_li

# --- 3. THE "STRICT ALIGNMENT" ENGINE ---
def main():
    # Load Resume
    with fitz.open("resume.pdf") as doc:
        resume_text = "".join([p.get_text() for p in doc])

    # Connect to Sheet
    client = get_google_client()
    sheet = client.open('JobTracker_2026').sheet1
    
    # Get Existing Data to avoid dupes
    existing_data = sheet.get_all_values()
    existing_urls = set([row[6] for row in existing_data if len(row) > 6])

    # Fetch All Data Related Roles
    terms = ['data analyst', 'data engineer', 'data assistant', 'business intelligence', 'reporting analyst']
    raw_jobs = fetch_adzuna(terms) + fetch_linkedin(terms)
    
    # Geocoder setup (Specific to Norwich, UK)
    geolocator = Nominatim(user_agent="norwich_data_hunter_2026")
    home_coords = (52.6289, 1.2933) # Norwich, UK
    today = datetime.now().strftime('%Y-%m-%d')
    
    final_upload_batch = []
    seen_in_run = set()

    print(f"🧠 Aligning {len(raw_jobs)} jobs to columns...")
    for j in raw_jobs:
        if j['url'] in existing_urls or j['url'] in seen_in_run:
            continue
        seen_in_run.add(j['url'])

        # Calculate Score
        score = calculate_match(resume_text, f"{j['title']} {j['desc']}")
        
        # Calculate Distance (Cleaned for common UK location strings)
        dist_val = "Unknown"
        try:
            clean_loc = str(j['location']).split(',')[0].replace("Area", "").strip() + ", UK"
            loc_data = geolocator.geocode(clean_loc, timeout=3)
            if loc_data:
                d = geodesic(home_coords, (loc_data.latitude, loc_data.longitude)).miles
                dist_val = f"{round(d, 1)} miles"
        except: pass

        # --- THE FIX: FORCED COLUMN MAPPING ---
        # We create a list with exactly 8 items. No more, no less.
        row_to_upload = [
            score,                      # Col A: Score
            str(j['title'])[:100],      # Col B: Title
            str(j['company']),          # Col C: Company
            str(j['location']),         # Col D: Location
            dist_val,                   # Col E: Distance
            today,                      # Col F: Date Found
            str(j['url']),              # Col G: Link
            str(j['source'])            # Col H: Source
        ]
        final_upload_batch.append(row_to_upload)

    if final_upload_batch:
        final_upload_batch.sort(key=lambda x: x[0], reverse=True)
        sheet.append_rows(final_upload_batch)
        print(f"🚀 Success! Perfectly aligned {len(final_upload_batch)} jobs.")
    else:
        print("📭 No new jobs to add.")

if __name__ == "__main__":
    main()
