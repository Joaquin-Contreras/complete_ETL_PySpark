import os
import json
import requests
from time import sleep
from dotenv import load_dotenv

load_dotenv()


API_KEY = os.getenv("API_KEY")  
BASE_URL = "https://api.themoviedb.org/3"

RAW_DIR = "data/raw"

os.makedirs(RAW_DIR, exist_ok=True)

def discover_movie_ids(pages=5):
    ids = []
    for page in range(1, pages + 1):
        resp = requests.get(f"{BASE_URL}/discover/movie", params={
            "api_key": API_KEY,
            "language": "en-US",
            "sort_by": "popularity.desc",
            "page": page
        })
        data = resp.json()
        results = data.get("results", [])
        for m in results:
            ids.append(m["id"])
        sleep(0.2)
    return ids

def fetch_movie_details(movie_id):
    fields = [
        "credits", "genres", "videos", "images",
        "reviews", "similar", "keywords", "release_dates"
    ]
    resp = requests.get(f"{BASE_URL}/movie/{movie_id}", params={
        "api_key": API_KEY,
        "append_to_response": ",".join(fields)
    })
    return resp.json()