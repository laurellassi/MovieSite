import csv
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from decouple import config
from django.core.management.base import BaseCommand
from movies.models import Movie

MAX_WORKERS = 4
TMDB_KEY = config("TMDB_API_KEY")

class Command(BaseCommand):
    help = "Add TMDb data (overview, actors, directors) to movies"

    def handle(self, *args, **kwargs):
        print("Starting TMDb enrichment...", flush=True)
        
        # Build mapping of movie_id to tmdb_id from links.csv
        movie_to_tmdb = {}
        with open("links.csv", newline="", encoding="utf-8") as links_file:
            reader = csv.DictReader(links_file)
            for row in reader:
                if row.get("tmdb_id"):
                    movie_to_tmdb[int(row["movie_id"])] = row["tmdb_id"]
        
        # Prepare list of movies to process
        movies_to_process = [
            (movie, movie_to_tmdb[movie.movie_id])
            for movie in Movie.objects.all()
            if movie.movie_id in movie_to_tmdb
        ]
        
        total = len(movies_to_process)
        completed = 0
        
        print(f"Updating {total} movies...", flush=True)
        
        def fetch_movie_data(movie, tmdb_id):
            try:
                url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
                params = {"api_key": TMDB_KEY, "append_to_response": "credits"}
                r = requests.get(url, params=params, timeout=5)
                
                if r.status_code != 200:
                    return "missing"
                
                data = r.json()
                
                # Update overview
                overview = data.get("overview", "")
                if overview:
                    movie.overview = overview
                
                credits = data.get("credits", {})
                
                # Store actors and directors as strings
                directors = [
                    crew_member["name"]
                    for crew_member in credits.get("crew", [])
                    if crew_member.get("job") == "Director"
                ]
                
                actors = [
                    cast_member["name"]
                    for cast_member in credits.get("cast", [])[:5]
                ]
                
                movie.directors = directors
                movie.actors = actors
                movie.save()
                
                return "updated"
                
            except Exception as e:
                print(f"Error for {movie.title}: {e}", flush=True)
                return "error"
        
        # Process in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(fetch_movie_data, movie, tmdb_id)
                for movie, tmdb_id in movies_to_process
            ]
            
            for future in as_completed(futures):
                completed += 1
                future.result()
                
                if completed % 10 == 0 or completed == total:
                    print(f"Progress: {completed}/{total}", flush=True)
        
        print("TMDb enrichment complete!")