from typing import Optional
from sqlmodel import SQLModel, Field


class Movie(SQLModel, table=True):
    """
    Movie table reflecting the CSV columns.
    Some multi-value fields are stored as JSON strings (genres, production_companies, etc.)
    """

    id: Optional[int] = Field(default=None, primary_key=True)  # auto-incrementing ID
    tmdb_id: Optional[int] = None  # store the original TMDB dataset ID
    title: Optional[str] = None
    vote_average: Optional[float] = None
    vote_count: Optional[int] = None
    status: Optional[str] = None
    release_date: Optional[str] = (
        None  # original CSV has string dates; keep as str for simplicity
    )
    revenue: Optional[int] = None
    runtime: Optional[int] = None
    adult: Optional[bool] = None
    backdrop_path: Optional[str] = None
    budget: Optional[int] = None
    homepage: Optional[str] = None
    imdb_id: Optional[str] = None
    original_language: Optional[str] = None
    original_title: Optional[str] = None
    overview: Optional[str] = None
    popularity: Optional[float] = None
    poster_path: Optional[str] = None
    tagline: Optional[str] = None
    # store multi-value fields as JSON text
    genres: Optional[str] = None
    production_companies: Optional[str] = None
    production_countries: Optional[str] = None
    spoken_languages: Optional[str] = None
    # keywords: CSV comment asked to split by ", " -> store JSON list
    keywords: Optional[str] = None
