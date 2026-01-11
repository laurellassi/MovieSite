import csv
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from decouple import config
from django.core.management.base import BaseCommand
from django.conf import settings
from movies.models import Movie

# Configuration constants
MEDIA_DIR = os.path.join(settings.MEDIA_ROOT, "movie_posters")
TMDB_KEY = config("TMDB_API_KEY")
MAX_WORKERS = 5

class Command(BaseCommand):
    help = "Download movie posters from TMDB"

    def handle(self, *args, **kwargs):
        # Create media directory if it doesn't exist
        if not os.path.exists(MEDIA_DIR):
            os.makedirs(MEDIA_DIR)

        # Build mapping of movie_id to tmdb_id from links.csv
        movie_to_tmdb = {}
        with open("links.csv", newline="", encoding="utf-8") as links_file:
            reader = csv.DictReader(links_file)
            for row in reader:
                if row.get("tmdb_id"):
                    movie_to_tmdb[int(row["movie_id"])] = row["tmdb_id"]

        # Collect all movies that have a TMDB ID
        movies_to_process = []
        for movie in Movie.objects.all():
            tmdb_id = movie_to_tmdb.get(movie.movie_id)
            if tmdb_id:
                movies_to_process.append((movie, tmdb_id))

        # Function to download poster for a single movie
        def fetch_image(movie, tmdb_id):
            image_path = os.path.join(MEDIA_DIR, f"{movie.movie_id}.jpg")

            # Skip download if image already exists locally
            if os.path.exists(image_path):
                # Just update database reference if needed
                if not movie.poster:
                    movie.poster = f"movie_posters/{movie.movie_id}.jpg"
                    movie.save()
                return "skipped"

            # Build TMDB API request
            details_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
            params = {"api_key": TMDB_KEY}

            try:
                # Fetch movie details from TMDB
                r = requests.get(details_url, params=params, timeout=5)
                if r.status_code != 200:
                    return "missing"

                data = r.json()
                poster_path = data.get("poster_path")

                # Check if poster exists in TMDB data
                if not poster_path:
                    return "missing"

                # Download the poster image
                image_url = f"https://image.tmdb.org/t/p/w200{poster_path}"
                img = requests.get(image_url, timeout=5)

                if img.status_code == 200:
                    # Save image to disk
                    with open(image_path, "wb") as f:
                        f.write(img.content)

                    # Update database with poster path
                    movie.poster = f"movie_posters/{movie.movie_id}.jpg"
                    movie.save()
                    print(f"Downloaded: {movie.title}", flush=True)
                    return "downloaded"

            except Exception as e:
                print(f"Error for {movie.title}: {e}", flush=True)
                return "error"

        # Download posters in parallel using thread pool
        total = len(movies_to_process)
        print(f"Starting download for {total} movies...", flush=True)

        completed = 0

        try:
            # Create thread pool executor
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all download tasks
                futures = [
                    executor.submit(fetch_image, movie, tmdb_id)
                    for movie, tmdb_id in movies_to_process
                ]

                # Process completed tasks and show progress
                for future in as_completed(futures):
                    completed += 1
                    future.result()

                    # Print progress every 10 movies or at completion
                    if completed % 10 == 0 or completed == total:
                        print(f"Progress: {completed}/{total}", flush=True)

        except KeyboardInterrupt:
            # Handle graceful shutdown on Ctrl+C
            print("\nInterrupted. Exiting cleanly.", flush=True)
            return

        print("All posters processed!", flush=True)
