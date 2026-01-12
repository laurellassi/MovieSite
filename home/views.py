from django.shortcuts import render
from movies.models import Movie
from django.db.models import Avg, Count, Max
import random
from movies.search_movies import search_movies

# Constant
MAX_MOVIES = 13

def home(request):
    # Get search query from URL parameters
    query = request.GET.get("q", "").strip()
    search_results = []
    
    # Perform search if query exists
    if query:
        search_results = search_movies(query) 

    # Get action movies
    action_movies = Movie.objects.filter(genres__contains=['Action'])[:MAX_MOVIES]

    # Get hidden gems
    hidden_gems = Movie.objects.annotate(
        avg_rating=Avg('rating__rating'),
        rating_count=Count('rating')
    ).filter(
        rating_count__gte=10,
        rating_count__lte=50,
        avg_rating__gte=4.0
    ).order_by('-avg_rating')[:MAX_MOVIES]

    # Get random movies
    max_id = Movie.objects.aggregate(max_id=Max('movie_id'))['max_id']
    start_id = random.randint(1, max_id) if max_id else 1
    random_movies = Movie.objects.filter(movie_id__gte=start_id).order_by('movie_id')[:MAX_MOVIES]

    # Movie data, will be passed to the template
    movie_data = [
        {'title': 'ACTION MOVIES', 'id': 'action', 'movies': action_movies},
        {'title': 'HIDDEN GEMS', 'id': 'gems', 'movies': hidden_gems},
        {'title': 'RANDOM', 'id': 'random', 'movies': random_movies},
    ]

    return render(request, 'home/home.html', {
        'query': query,
        'search_results': search_results,
        'movie_data': movie_data
    })
