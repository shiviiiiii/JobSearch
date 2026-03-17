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

# --- 1. AUTH & AI SETUP ---
def get_google_client():
    try:
        secret_json = os.getenv('GOOGLE_SHEET_CREDENTIALS')
        creds_dict = json.loads(secret_json)
        return gspread.authorize(Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))
    except Exception as e:
        print(f"❌ Google Auth Error: {e}")
        return None

print("🤖 Loading AI Model...")
model = SentenceTransformer('all-MiniLM-L6-v2')

def calculate_match(resume_text, job_blob):
    embeddings = model.encode([resume_text, job_blob], convert_to_tensor=True)
    return round(float(util.cos_sim(embeddings[0], embeddings[1])) * 100, 2)

# --- 2. DATA STANDARDIZATION ---
def clean_job_data(title, company, location, desc, url, source):
    """Ensures every job has the exact same fields before processing"""
    return {
        'title': str(title or "Unknown Title"),
        'company': str(company or "Unknown Company"),
        'location': str(location or "UK"),
        'description': str(desc or "No description provided"),
        'url': str(url or ""),
        'source': str(source)
    }

# --- 3. THE FETCHERS ---
def fetch_adzuna():
    APP_ID = os.getenv('ADZUNA_APP_ID')
    APP_KEY = os.getenv('ADZUNA_APP_KEY')
    if not APP_ID or not APP_KEY:
        print("⚠️ Adzuna Keys missing. Skipping Adzuna...")
        return []
    
    print("🛰️ Adzuna: Fetching...")
    url = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
    params = {'app_id': APP_ID, 'app_key': APP_KEY, 'results_per_page': 50, 'what': 'data analyst', 'where': 'UK'}
    
    try:
        r = requests.get(url, params=params, timeout=15)
        results = r.json().get('results', [])
        return [clean_job_data(j.get('title'), j.get('company', {}).get('display_name'), 
                               j.get('location', {}).get('display_name'), j.get('description'), 
                               j.get('redirect_url'), 'Adzuna') for j in results]
    except Exception as e:
        print(f"❌ Adzuna Failed: {e}")
        return []

def fetch_linkedin():
    print("🛰️ LinkedIn: Scraping...")
    try:
        df = scrape_jobs(
            site_name=["linkedin"],
            search_term='data analyst',
            location="United Kingdom",
            results_wanted=25,
            hours_old=72,
            linkedin_fetch_description=True
        )
        return [clean_job_data(row['title'], row['company'], row['location'], 
                               row['description'], row['job_url'], 'LinkedIn') for _, row in df.iterrows()]
    except Exception as e:
        print(f"❌ LinkedIn Failed: {e}")
        return []

# --- 4. MAIN PROCESS ---
def main():
    # Load Resume
    try:
        with fitz.open("resume.pdf") as doc:
            resume_text = "".join([p.get_text() for p in doc])
    except:
        print("❌ Could not find resume.pdf"); return

    # Gather & Deduplicate by URL
    all_raw = fetch_adzuna() + fetch_linkedin()
    if not all_raw:
        print("📭 No jobs found."); return

    # Processing Tools
    geolocator = Nominatim(user_agent="norwich_job_bot_2026")
    norwich_coords = (52.6289, 1.2933)
    today = datetime.now().strftime('%Y-%m-%d')
    
    processed_rows = []
    seen_urls = set()

    print(f"🧠 Processing {len(all_raw)} jobs...")
    for j in all_raw:
        if j['url'] in seen_urls: continue
        seen_urls.add(j['url'])

        # AI Scoring
        score = calculate_match(resume_text, f"{j['title']} {j['description']}")
        
        # Distance Logic
        dist_str = "N/A"
        try:
            city = j['location'].split(',')[0].replace("Area", "").strip()
            loc = geolocator.geocode(f"{city}, UK", timeout=2)
            if loc:
                d = geodesic(norwich_coords, (loc.latitude, loc.longitude)).miles
                dist_str = f"{round(d, 1)} miles"
            time.sleep(1) # Respect Geocoder limits
        except: pass

        # STRICT COLUMN MAPPING [Score, Title, Company, Location, Distance, Date, Link, Source]
        processed_rows.append([
            score, j['title'], j['company'], j['location'], 
            dist_str, today, j['url'], j['source']
        ])

    # Sort by Match Score (Highest first)
    processed_rows.sort(key=lambda x: x[0], reverse=True)

    # Final Upload
    client = get_google_client()
    if client:
        sheet = client.open('JobTracker_2026').sheet1
        sheet.append_rows(processed_rows)
        print(f"🚀 Successfully added {len(processed_rows)} jobs in correct columns!")

if __name__ == "__main__":
    main()
