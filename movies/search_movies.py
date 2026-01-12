from django.contrib.postgres.search import SearchQuery, SearchRank, TrigramSimilarity
from django.db.models import Q
from movies.models import Movie

def search_movies(query):
    # Turn the string into a Postgres search query
    search_query = SearchQuery(query)
    
    # Search movies using full-text and fuzzy matching
    return Movie.objects.annotate(
        # How well the full-text vector matches
        rank=SearchRank("search_vector", search_query),
        
        # How similar the title is typo tolerant
        similarity=TrigramSimilarity("title", query),
    ).filter(
        # Accept if either system finds a good match
        Q(rank__gt=0.1) | Q(similarity__gt=0.2)  
    ).order_by("-rank", "-similarity")[:50]  
