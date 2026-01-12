import datetime
from django.db import models
from django.utils import timezone
from django.contrib.auth.models import AbstractUser
from django.core.validators import MaxValueValidator, MinValueValidator
from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVector, SearchVectorField
from django.contrib.postgres.fields import ArrayField
from django.db.models.signals import post_save
from django.dispatch import receiver

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
    
class Actor(models.Model):
    name = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.name 
    
class Director(models.Model):
    name = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.name   
    
class Movie(models.Model):
    # Movies imported from MovieLens dataset
    movie_id = models.IntegerField(primary_key=True)
    title = models.CharField(max_length=255, db_index=True)
    description = models.TextField(blank=True)  # Short description / tagline
    overview = models.TextField(blank=True)     # Longer synopsis
    poster = models.ImageField(upload_to='movie_posters/', blank=True, null=True)
    directors = ArrayField(models.CharField(max_length=255), blank=True, default=list)
    actors = ArrayField(models.CharField(max_length=255), blank=True, default=list)
    genres = ArrayField(models.CharField(max_length=100), blank=True, default=list)
    tags = ArrayField(models.CharField(max_length=100), blank=True, default=list)

    # Full-text search vector for efficient searching
    search_vector = SearchVectorField(null=True)
    
    class Meta:
        indexes = [
            # GIN index for full-text search on search_vector
            GinIndex(fields=["search_vector"], name="movie_search_vector_gin"),
            
            # GIN index with trigram ops for fuzzy title matching
            GinIndex(
                name="movie_title_trgm_gin",
                fields=["title"],
                opclasses=["gin_trgm_ops"],
            ),
        ]
        
    def __str__(self):
        return self.title

# Signal that automatically runs whenever a Movie is saved
@receiver(post_save, sender=Movie)
def update_movie_search(sender, instance, **kwargs):
    # Update the search_vector field for full-text search
    Movie.objects.filter(pk=instance.pk).update(
        search_vector=
            # Weight A = highest importance (title, directors)
            SearchVector("title", weight="A") +
            # Weight B = medium importance (overview, description, cast)
            SearchVector("overview", weight="B") +
            SearchVector("description", weight="B") +
            SearchVector("directors", weight="A") +
            SearchVector("actors", weight="B") +
            # Weight C = lower importance (genres, tags)
            SearchVector("genres", weight="C") +
            SearchVector("tags", weight="C")
    )

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