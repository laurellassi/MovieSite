import datetime
from django.db import models
from django.utils import timezone
from django.contrib.auth.models import AbstractUser
from django.core.validators import MaxValueValidator, MinValueValidator

def custom_user():
    return User.objects.get(name='deleted_user')

class Genre(models.Model):
    name = models.CharField(max_length=50, unique=True)

    def __str__(self):
        return self.name
    
class User(AbstractUser):
    # Initial users are from MovieLens 
    profile_pic = models.ImageField(upload_to='profiles/', blank=True)

    def __str__(self):
        return self.username

class Movie(models.Model):
    # Movies are from MovieLens
    movie_id = models.IntegerField(primary_key=True)
    title = models.CharField(max_length=255) 
    genres = models.ManyToManyField(Genre) 
    poster = models.ImageField(upload_to='movie_posters/', blank=True, null=True)
    
    def __str__(self):
        return self.title

class Rating(models.Model):
    # Initial ratings are from MovieLens
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    user = models.ForeignKey(User, on_delete=models.SET(custom_user))
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE)
    rating = models.IntegerField(
        default=0,
        validators=[MaxValueValidator(5), MinValueValidator(0)]
    )

    def __str__(self):
        return f"{self.movie.title} - {self.rating}"

