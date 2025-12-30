from django.shortcuts import render
from movies.models import Movie 

def home(request):
    movies = Movie.objects.all()[:50]

    context = {
        'movies': movies
    }
    return render(request, 'home/home.html', context)