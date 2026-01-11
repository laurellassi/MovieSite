import csv
from django.core.management.base import BaseCommand
from movies.models import Movie

class Command(BaseCommand):
    help = "Import movie tags from tags.csv"

    def handle(self, *args, **kwargs):
        print("Importing tags...", flush=True)
        
        # Track number of tags processed
        count = 0
        
        # Open and read the CSV file
        with open("tags.csv", newline="", encoding="utf-8") as tags_file:
            reader = csv.DictReader(tags_file)
            
            # Process each tag row
            for row in reader:
                # Extract tag data from CSV
                movie_id = int(row["movieId"])
                tag_text = row["tag"].strip()
                
                try:
                    # Find the movie
                    movie = Movie.objects.get(movie_id=movie_id)
                    
                    # Add tag to array if not already present
                    if tag_text not in movie.tags:
                        movie.tags.append(tag_text)
                        movie.save()
                    
                    # Increment counter
                    count += 1
                    
                    # Print progress every 1000 tags
                    if count % 1000 == 0:
                        print(f"Processed {count} tags...", flush=True)
                
                # Skip if movie doesn't exist in database
                except Movie.DoesNotExist:
                    print(f"Movie {movie_id} not found, skipping.", flush=True)
        
        # Print completion message
        print(f"Finished! Imported {count} tags.", flush=True)