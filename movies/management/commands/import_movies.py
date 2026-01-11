import csv
from django.core.management.base import BaseCommand
from movies.models import Movie, Genre

class Command(BaseCommand):
    help = "Import movies and genres from movies.csv"

    def handle(self, *args, **kwargs):
        print("Importing movies and genres...", flush=True)
        
        # Track number of movies processed
        count = 0
        
        # Open and read the CSV file
        with open('movies.csv', newline='', encoding='utf-8') as movies_file:
            reader = csv.DictReader(movies_file)
            
            # Process each row in the CSV
            for row in reader:
                # Extract movie data from CSV row
                movie_id = int(row['movie_id'])
                title = row['title']
                genre_names = row['genres'].split('|')
                
                # Create or get the movie (avoid duplicates)
                movie, created = Movie.objects.get_or_create(
                    movie_id=movie_id,
                    defaults={'title': title}
                )
                
                # Add each genre to the movie
                for name in genre_names:
                    genre, _ = Genre.objects.get_or_create(name=name)
                    movie.genres.add(genre)
                
                # Increment counter
                count += 1
                
                # Print progress every 100 movies
                if count % 100 == 0:
                    print(f"Processed {count} movies...", flush=True)
        
        # Print completion message
        print(f"Finished! Imported {count} movies.", flush=True)