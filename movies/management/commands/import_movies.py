import csv
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from decouple import config
from django.core.management.base import BaseCommand
from django.conf import settings
from movies.models import Movie, Genre

MEDIA_DIR = os.path.join(settings.MEDIA_ROOT, "movie_posters")
TMDB_KEY = config('TMDB_API_KEY')
MAX_WORKERS = 5

class Command(BaseCommand):
    help = "Import movies from CSV and download TMDB images with progress"

    def handle(self, *args, **kwargs):
        if not os.path.exists(MEDIA_DIR):
            os.makedirs(MEDIA_DIR)

        # Maps movie_id to tmdb_id
        movie_to_tmdb = {}
        with open('links.csv', newline='', encoding='utf-8') as links_file:
            reader = csv.DictReader(links_file)
            for row in reader:
                if row.get('tmdb_id'):
                    movie_to_tmdb[int(row['movie_id'])] = row['tmdb_id']

        # Load movies and add genres
        movies_to_process = []
        with open('movies.csv', newline='', encoding='utf-8') as movies_file:
            reader = csv.DictReader(movies_file)
            for row in reader:
                movie_id = int(row['movie_id'])
                title = row['title']
                genre_names = row['genres'].split('|')

                movie, _ = Movie.objects.get_or_create(
                    movie_id=movie_id,
                    defaults={'title': title}
                )

                for name in genre_names:
                    genre, _ = Genre.objects.get_or_create(name=name)
                    movie.genres.add(genre)

                tmdb_id = movie_to_tmdb.get(movie_id)
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
                    print(f"No TMDB data for {movie.title}", flush=True)
                    return "missing"
                    
                data = r.json()
                poster_path = data.get("poster_path")
                
                if not poster_path:
                    print(f"No poster for {movie.title}", flush=True)
                    return "missing"

                image_url = f"https://image.tmdb.org/t/p/w200{poster_path}"

                img = requests.get(image_url, timeout=5)
                if img.status_code == 200:
                    with open(image_path, "wb") as f:
                        f.write(img.content)
                    movie.poster = f"movie_posters/{movie.movie_id}.jpg"
                    movie.save()
                    print(f"Saved: {movie.title}", flush=True)
                    return "downloaded"
                    
            except Exception as e:
                print(f"Error for {movie.title}: {e}", flush=True)
                return "error"

        # Fetch images in parallel with progress
        total = len(movies_to_process)
        print(f"Starting image download for {total} movies...", flush=True)
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
            print("\nInterrupted by user. Exiting cleanly.", flush=True)
            return

        print("All movies processed.", flush=True)