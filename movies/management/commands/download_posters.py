import csv
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from decouple import config
from django.core.management.base import BaseCommand
from django.conf import settings
from movies.models import Movie

MEDIA_DIR = os.path.join(settings.MEDIA_ROOT, "movie_posters")
TMDB_KEY = config('TMDB_API_KEY')
MAX_WORKERS = 5

class Command(BaseCommand):
    help = "Download movie posters from TMDB"

    def handle(self, *args, **kwargs):
        if not os.path.exists(MEDIA_DIR):
            os.makedirs(MEDIA_DIR)

        # Map movie_id to tmdb_id
        movie_to_tmdb = {}
        with open('links.csv', newline='', encoding='utf-8') as links_file:
            reader = csv.DictReader(links_file)
            for row in reader:
                if row.get('tmdb_id'):
                    movie_to_tmdb[int(row['movie_id'])] = row['tmdb_id']

        # Get all movies that need posters
        movies_to_process = []
        for movie in Movie.objects.all():
            tmdb_id = movie_to_tmdb.get(movie.movie_id)
            if tmdb_id:
                movies_to_process.append((movie, tmdb_id))

        # Function to fetch and save image
        def fetch_image(movie, tmdb_id):
            image_path = os.path.join(MEDIA_DIR, f"{movie.movie_id}.jpg")
            
            # If image exists, just update database
            if os.path.exists(image_path):
                if not movie.poster:
                    movie.poster = f"movie_posters/{movie.movie_id}.jpg"
                    movie.save()
                return "skipped"
            
            details_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
            params = {"api_key": TMDB_KEY}
            
            try:
                r = requests.get(details_url, params=params, timeout=5)
                if r.status_code != 200:
                    return "missing"
                    
                data = r.json()
                poster_path = data.get("poster_path")
                
                if not poster_path:
                    return "missing"

                image_url = f"https://image.tmdb.org/t/p/w200{poster_path}"

                img = requests.get(image_url, timeout=5)
                if img.status_code == 200:
                    with open(image_path, "wb") as f:
                        f.write(img.content)
                    movie.poster = f"movie_posters/{movie.movie_id}.jpg"
                    movie.save()
                    print(f"Downloaded: {movie.title}", flush=True)
                    return "downloaded"
                    
            except Exception as e:
                print(f"Error for {movie.title}: {e}", flush=True)
                return "error"

        # Download images in parallel
        total = len(movies_to_process)
        print(f"Starting download for {total} movies...", flush=True)
        completed = 0

        try:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(fetch_image, movie, tmdb_id)
                    for movie, tmdb_id in movies_to_process
                ]
                
                for future in as_completed(futures):
                    completed += 1
                    future.result()
                    if completed % 10 == 0 or completed == total:
                        print(f"Progress: {completed}/{total}", flush=True)
                        
        except KeyboardInterrupt:
            print("\nInterrupted. Exiting cleanly.", flush=True)
            return

        print("All posters processed!", flush=True)