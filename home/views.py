from django.shortcuts import render
from movies.models import Movie 
from django.db.models import Avg, Count, Max, Q
import random
from django.contrib.postgres.search import SearchQuery, SearchRank, TrigramSimilarity
from movies.search_movies import search_movies

def home(request):
    # Get 13 action movies
    action_movies = Movie.objects.filter(genres__name='Action')[:13]
    
    # Find hidden gems (highly rated but not widely known)
    hidden_gems = Movie.objects.annotate(
        avg_rating=Avg('rating__rating'),
        rating_count=Count('rating')
    ).filter(
        rating_count__gte=10,  # At least 10 ratings
        rating_count__lte=50,  # No more than 50 ratings
        avg_rating__gte=4.0    # Average rating of 4.0 or higher
    ).order_by('-avg_rating')[:13]
    
    # Get random movies starting from a random ID
    max_id = Movie.objects.aggregate(max_id=Max('movie_id'))['max_id']
    start_id = random.randint(1, max_id) if max_id else 1
    random_movies = Movie.objects.filter(movie_id__gte=start_id).order_by('movie_id')[:13]
    
    # Package all movie sections for the template
    context = {
        'movie_data': [
            {'title': 'ACTION MOVIES', 'id': 'action', 'movies': action_movies},
            {'title': 'HIDDEN GEMS', 'id': 'gems', 'movies': hidden_gems},
            {'title': 'RANDOM', 'id': 'random', 'movies': random_movies},
        ]
    }
    
    return render(request, 'home/home.html', context)

def search_view(request):
    # Get search query from URL parameter 'q'
    query = request.GET.get("q", "")
    
    # Perform the search using the search_movies function
    results = search_movies(query)
    
    # Render the search results template
    return render(request, "search.html", {"results": results})