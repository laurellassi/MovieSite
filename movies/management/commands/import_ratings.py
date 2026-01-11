import csv
from django.core.management.base import BaseCommand
from movies.models import Movie, User, Rating


class Command(BaseCommand):
    help = "Import ratings from ratings.csv"

    def handle(self, *args, **kwargs):
        print("Importing ratings...", flush=True)

        # Track import statistics
        ratings_imported = 0
        ratings_skipped = 0

        # Open and read the CSV file
        with open("ratings.csv", newline="", encoding="utf-8") as ratings_file:
            reader = csv.DictReader(ratings_file)

            # Process each rating row
            for row in reader:
                # Extract data from CSV row
                user_id = int(row["user_id"])
                movie_id = int(row["movie_id"])
                rating_value = float(row["rating"])

                try:
                    # Create user if it doesn't exist
                    user, _ = User.objects.get_or_create(
                        id=user_id,
                        defaults={"username": f"user_{user_id}", "password": "!"},
                    )

                    # Get the movie (will raise exception if not found)
                    movie = Movie.objects.get(movie_id=movie_id)

                    # Create new rating or update existing one
                    rating, created = Rating.objects.update_or_create(
                        user=user, movie=movie, defaults={"rating": int(rating_value)}
                    )

                    # Update counters based on whether rating was created or updated
                    if created:
                        ratings_imported += 1
                    else:
                        ratings_skipped += 1

                    # Print progress every 1000 ratings
                    if (ratings_imported + ratings_skipped) % 1000 == 0:
                        print(
                            f"Processed {ratings_imported + ratings_skipped} ratings...",
                            flush=True,
                        )

                # Handle case where movie doesn't exist
                except Movie.DoesNotExist:
                    ratings_skipped += 1

                # Handle any other errors
                except Exception as e:
                    print(f"Error: {e}", flush=True)
                    ratings_skipped += 1

        # Print final summary
        print(
            f"Complete! Imported: {ratings_imported}, Skipped: {ratings_skipped}",
            flush=True,
        )
