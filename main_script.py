import sys
import os
import subprocess

# --- FORCE PATH SEARCH ---
# This ensures the script finds the libraries GitHub just installed
def emergency_install():
    try:
        from jobspy import scrape_jobs
    except ImportError:
        print("🛠️ Module not found. Running emergency install...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "python-jobspy"])

emergency_install()

import json
import time
import fitz  # PyMuPDF
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from sentence_transformers import SentenceTransformer, util
from jobspy import scrape_jobs
from geopy.geocoders import Nominatim
from geopy.distance import geodesic

# --- 1. SETUP & AUTH ---
def get_google_client():
    secret_json = os.getenv('GOOGLE_SHEET_CREDENTIALS')
    if not secret_json:
        raise ValueError("SECRET MISSING: GOOGLE_SHEET_CREDENTIALS")
    creds_dict = json.loads(secret_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

print("Loading AI Model...")
model = SentenceTransformer('all-MiniLM-L6-v2')

def calculate_match(resume_text, job_blob):
    embeddings = model.encode([resume_text, job_blob], convert_to_tensor=True)
    score = util.cos_sim(embeddings[0], embeddings[1])
    return round(float(score) * 100, 2)

def extract_resume_text():
    resume_path = "resume.pdf"
    text = ""
    try:
        with fitz.open(resume_path) as doc:
            for page in doc:
                text += page.get_text()
    except Exception as e:
        print(f"Resume Error: {e}")
    return text

# --- 2. SCRAPER ---
def fetch_linkedin():
    print("🛰️ Fetching LinkedIn...")
    try:
        jobs = scrape_jobs(
            site_name=["linkedin"],
            search_term='data analyst',
            location="United Kingdom",
            results_wanted=30,
            hours_old=48,
            linkedin_fetch_description=True
        )
        return jobs
    except Exception as e:
        print(f"Scraper Error: {e}")
        return pd.DataFrame()

# --- 3. MAIN ---
def main():
    resume_text = extract_resume_text()
    geolocator = Nominatim(user_agent="norwich_job_bot_2026")
    norwich_coords = (52.6289, 1.2933) 

    li_jobs = fetch_linkedin()
    if li_jobs.empty:
        print("No jobs found.")
        return

    scored_list = []
    for _, row in li_jobs.iterrows():
        title = str(row.get('title', ''))
        desc = str(row.get('description', ''))
        location_name = str(row.get('location', 'UK'))
        
        score = calculate_match(resume_text, f"{title} {desc}")
        
        # Distance
        dist_str = "Unknown"
        try:
            loc = geolocator.geocode(f"{location_name}, UK", timeout=10)
            if loc:
                dist = geodesic(norwich_coords, (loc.latitude, loc.longitude)).miles
                dist_str = f"{round(dist, 1)} miles"
            time.sleep(1.1)
        except: pass

        scored_list.append([score, title, row.get('company'), location_name, dist_str, row.get('job_url')])

    # Upload
    print("Uploading...")
    gc = get_google_client()
    sh = gc.open('JobTracker_2026').sheet1
    sh.append_rows(scored_list)
    print("✅ Done!")

if __name__ == "__main__":
    main()
