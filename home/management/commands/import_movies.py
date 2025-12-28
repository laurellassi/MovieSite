import csv
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from decouple import config
from django.core.management.base import BaseCommand
from home.models import Movie, Genre

MEDIA_DIR = 'media/movie_posters/'
TMDB_KEY = config('TMDB_API_KEY')
MAX_WORKERS = 5  # number of parallel threads

class Command(BaseCommand):
    help = "Import movies from CSV and download TMDB images with progress"

    def handle(self, *args, **kwargs):
        if not os.path.exists(MEDIA_DIR):
            os.makedirs(MEDIA_DIR)

        # Maps movie_id to imdb_id
        movie_to_tmdb = {}
        with open('links.csv', newline='', encoding='utf-8') as links_file:
            reader = csv.DictReader(links_file)
            for row in reader:
                movie_id = int(row['movie_id'])
                tmdb_id = row.get('tmdb_id')
                if tmdb_id:
                    movie_to_tmdb[movie_id] = tmdb_id

        # Load movies and add genres
        movies_to_process = []
        with open('movies.csv', newline='', encoding='utf-8') as movies_file:
            reader = csv.DictReader(movies_file)
            for row in reader:
                movie_id = int(row['movie_id'])
                title = row['title']
                genre_names = row['genres'].split('|')

                # Create or get Movie
                movie, _ = Movie.objects.get_or_create(
                    movie_id=movie_id,
                    defaults={'title': title}
                )

                # Add genres
                for name in genre_names:
                    genre, _ = Genre.objects.get_or_create(name=name)
                    movie.genres.add(genre)

                # Only process movies with a TMDB ID
                tmdb_id = movie_to_tmdb.get(movie_id)
                if tmdb_id:
                    movies_to_process.append((movie, tmdb_id))

        # Function to fetch and save image
        def fetch_image(movie, tmdb_id):
            image_url = f"https://image.tmdb.org/t/p/w200/{tmdb_id}.jpg"
            image_path = os.path.join(MEDIA_DIR, f"{movie.movie_id}.jpg")

            if os.path.exists(image_path):
                return "skipped"

            print(f"Downloading: {movie.title}", flush=True)

            try:
                r = requests.get(image_url, timeout=5)
                if r.status_code == 200:
                    with open(image_path, 'wb') as f:
                        f.write(r.content)
                    movie.image_url = image_path
                    movie.save()
                    print(f"Saved: {movie.title}", flush=True)
                    return "downloaded"
                else:
                    print(f"No image for: {movie.title}", flush=True)
                    return "missing"
            except requests.exceptions.Timeout:
                print(f"Timeout: {movie.title}", flush=True)
                return "timeout"
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
