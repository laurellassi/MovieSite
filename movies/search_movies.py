from django.contrib.postgres.search import (
    SearchVector,
    SearchQuery,
    SearchRank,
    TrigramSimilarity,
)
from django.db.models import Q, F, Func, Case, When, IntegerField, Value, CharField
from movies.models import Movie

# Search result limits for each stage
MAX_EXACT_MATCHES = 10
MAX_PREFIX_MATCHES = 20
MAX_FULLTEXT_MATCHES = 70
MAX_TOTAL_RESULTS = 100

# Movie search using PostgreSQL full-text search and trigram similarity.
# Returns movies ranked by relevance across multiple search strategies.
def search_movies(query):
    # Clean and validate the search query
    q = (query or "").strip()
    if not q:
        return Movie.objects.none()
    
    # Convert array fields to text strings for full-text search
    directors_text = Func(
        F("directors"), Value(","), function="array_to_string", output_field=CharField()
    )
    actors_text = Func(
        F("actors"), Value(","), function="array_to_string", output_field=CharField()
    )
    genres_text = Func(
        F("genres"), Value(","), function="array_to_string", output_field=CharField()
    )
    tags_text = Func(
        F("tags"), Value(","), function="array_to_string", output_field=CharField()
    )
    
    # Build a weighted search vector combining multiple fields
    # Weight A = highest importance (title, directors)
    # Weight B = medium importance (overview, description, actors)
    # Weight C = lower importance (genres, tags)
    dynamic_vector = (
        SearchVector("title", weight="A")
        + SearchVector("overview", weight="B")
        + SearchVector("description", weight="B")
        + SearchVector(directors_text, weight="A")
        + SearchVector(actors_text, weight="B")
        + SearchVector(genres_text, weight="C")
        + SearchVector(tags_text, weight="C")
    )
    
    # Create search query using websearch type for better multi-word handling
    search_query = SearchQuery(q, search_type="websearch")
    
    # Use stricter threshold for very short queries to reduce noise
    trigram_threshold = 0.3 if len(q) <= 3 else 0.05
    
    # Annotate all movies with relevance rank and title similarity scores
    qs = Movie.objects.annotate(
        rank=SearchRank(dynamic_vector, search_query),
        similarity=TrigramSimilarity("title", q),
    )
    
    # Exact title matches (highest priority)
    exact_pks = list(qs.filter(title__iexact=q).values_list("pk", flat=True)[:MAX_EXACT_MATCHES])
    
    # Title prefix matches
    starts_pks = list(
        qs.filter(title__istartswith=q)
        .exclude(pk__in=exact_pks)
        .values_list("pk", flat=True)[:MAX_PREFIX_MATCHES]
    )
    
    # Full-text search results ranked by relevance
    ft_pks = list(
        qs.filter(rank__gt=0)
        .exclude(pk__in=exact_pks + starts_pks)
        .order_by("-rank")
        .values_list("pk", flat=True)[:MAX_FULLTEXT_MATCHES]
    )
    
    # Trigram similarity fallback for fuzzy matching
    remaining_slots = MAX_TOTAL_RESULTS - len(exact_pks) - len(starts_pks) - len(ft_pks)

    trigram_pks = list(
        qs.filter(similarity__gt=trigram_threshold)
        .exclude(pk__in=exact_pks + starts_pks + ft_pks)
        .order_by("-similarity")
        .values_list("pk", flat=True)[:remaining_slots]
    )
    
    # Combine all results in priority order
    ordered_pks = exact_pks + starts_pks + ft_pks + trigram_pks
    
    if not ordered_pks:
        return Movie.objects.none()
    
    # Preserve the ordering when fetching the final queryset
    whens = [When(pk=pk, then=pos) for pos, pk in enumerate(ordered_pks)]
    ordered_qs = Movie.objects.filter(pk__in=ordered_pks).order_by(
        Case(*whens, output_field=IntegerField())
    )
    
    return ordered_qs