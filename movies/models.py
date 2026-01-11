import datetime
from django.db import models
from django.utils import timezone
from django.contrib.auth.models import AbstractUser
from django.core.validators import MaxValueValidator, MinValueValidator

# Default user for when a user is deleted
def custom_user():
    return User.objects.get(name='deleted_user')

class Genre(models.Model):
    # Unique genre name (e.g., Action, Comedy)
    name = models.CharField(max_length=50, unique=True)
    
    def __str__(self):
        return self.name
    
class User(AbstractUser):
    # Custom user model extending Django's built-in user
    # Initial users are from MovieLens dataset
    profile_pic = models.ImageField(upload_to='profiles/', blank=True)
    
    def __str__(self):
        return self.username

class Movie(models.Model):
    # Movies imported from MovieLens dataset
    movie_id = models.IntegerField(primary_key=True)
    title = models.CharField(max_length=255) 
    genres = models.ManyToManyField(Genre)  # A movie can have multiple genres
    poster = models.ImageField(upload_to='movie_posters/', blank=True, null=True)
    
    def __str__(self):
        return self.title

class Rating(models.Model):
    # User ratings for movies
    # Initial ratings are from MovieLens dataset
    
    # Timestamps for tracking when rating was created/updated
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Foreign keys linking to user and movie
    user = models.ForeignKey(User, on_delete=models.SET(custom_user))  # Keep rating if user deleted
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE)  # Delete rating if movie deleted
    
    # Rating value (0-5 scale)
    rating = models.IntegerField(
        default=0,
        validators=[MaxValueValidator(5), MinValueValidator(0)]
    )
    
    def __str__(self):
        return f"{self.movie.title} - {self.rating}"