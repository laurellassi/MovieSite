import csv
from django.core.management.base import BaseCommand
from movies.models import Movie, Genre

class Command(BaseCommand):
    help = "Import movies and genres from movies.csv"

    def handle(self, *args, **kwargs):
        print("Importing movies and genres...", flush=True)
        count = 0
        
        with open('movies.csv', newline='', encoding='utf-8') as movies_file:
            reader = csv.DictReader(movies_file)
            for row in reader:
                movie_id = int(row['movie_id'])
                title = row['title']
                genre_names = row['genres'].split('|')

                movie, created = Movie.objects.get_or_create(
                    movie_id=movie_id,
                    defaults={'title': title}
                )

                for name in genre_names:
                    genre, _ = Genre.objects.get_or_create(name=name)
                    movie.genres.add(genre)
                
                count += 1
                if count % 100 == 0:
                    print(f"Processed {count} movies...", flush=True)

        print(f"Finished! Imported {count} movies.", flush=True)