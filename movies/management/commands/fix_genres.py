import csv
from django.core.management.base import BaseCommand
from movies.models import Movie

class Command(BaseCommand):
    help = "Fix genres for existing movies"
    
    def handle(self, *args, **kwargs):
        # Print starting message
        print("Fixing movie genres...", flush=True)
        
        # Initialize counters to track progress
        count = 0      # Total movies processed
        updated = 0    # Movies successfully updated
        
        # Open the movies.csv file
        with open('movies.csv', newline='', encoding='utf-8') as movies_file:
            # Create a CSV reader that reads rows as dictionaries
            reader = csv.DictReader(movies_file)
            
            # Loop through each row in the CSV
            for row in reader:
                # Extract movie_id from the CSV row
                movie_id = int(row['movie_id'])
                
                # Extract genres and split by '|' into a list
                genre_names = row['genres'].split('|') if row['genres'] else []
                
                try:
                    # Try to find the movie in the database
                    movie = Movie.objects.get(movie_id=movie_id)
                    
                    # Update the genres field with the list of genre names
                    movie.genres = genre_names
                    
                    # Save the movie to the database
                    movie.save()
                    
                    # Increment the updated counter
                    updated += 1
                    
                except Movie.DoesNotExist:
                    # If the movie doesn't exist in the database, skip it
                    pass
                
                # Increment the total processed counter
                count += 1
                
                # Print progress update every 100 movies
                if count % 100 == 0:
                    print(f"Processed {count} movies, updated {updated}...", flush=True)
        
        # Print completion message with final statistics
        print(f"Finished! Processed {count} movies, updated {updated}.", flush=True)