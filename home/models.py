import datetime
from django.db import models
from django.utils import timezone

class Genre(models.Model):
    name = models.CharField(max_length = 50, unique = True)

    def __str__(self):
        return self.name

class Movie(models.Model):
    # From MovieLens
    movie_id = models.IntegerField(primary_key = True)
    title = models.CharField(max_length = 255) 
    genres = models.ManyToManyField(Genre) 
    poster = models.ImageField(upload_to='movie_posters/', blank=True, null=True)
    
    def __str__(self):
        return self.title

