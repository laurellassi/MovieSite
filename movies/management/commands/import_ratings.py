import csv
from django.core.management.base import BaseCommand
from movies.models import Movie, User, Rating

class Command(BaseCommand):
    help = "Import ratings from ratings.csv"

    def handle(self, *args, **kwargs):
        print("Importing ratings...", flush=True)
        ratings_imported = 0
        ratings_skipped = 0
        
        with open('ratings.csv', newline='', encoding='utf-8') as ratings_file:
            reader = csv.DictReader(ratings_file)
            for row in reader:
                user_id = int(row['user_id'])
                movie_id = int(row['movie_id'])
                rating_value = float(row['rating'])
                
                try:
                    # Create user if doesn't exist
                    user, _ = User.objects.get_or_create(
                        id=user_id,
                        defaults={
                            'username': f'user_{user_id}',
                            'password': '!'
                        }
                    )
                    
                    # Get the movie
                    movie = Movie.objects.get(movie_id=movie_id)
                    
                    # Create or update rating
                    rating, created = Rating.objects.update_or_create(
                        user=user,
                        movie=movie,
                        defaults={'rating': int(rating_value)}
                    )
                    
                    if created:
                        ratings_imported += 1
                    else:
                        ratings_skipped += 1
                    
                    # Progress update
                    if (ratings_imported + ratings_skipped) % 1000 == 0:
                        print(f"Processed {ratings_imported + ratings_skipped} ratings...", flush=True)
                        
                except Movie.DoesNotExist:
                    ratings_skipped += 1
                except Exception as e:
                    print(f"Error: {e}", flush=True)
                    ratings_skipped += 1

        print(f"Complete! Imported: {ratings_imported}, Skipped: {ratings_skipped}", flush=True)