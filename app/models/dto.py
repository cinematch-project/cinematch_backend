from datetime import date
from pydantic import BaseModel
from sqlmodel import SQLModel

from app.models.movie import Genre, Keyword, ProductionCompany, ProductionCountry


class RecommendMoviesRequest(BaseModel):
    ids: list[int]
    top_n: int | None = 10
    similarity_weight: float | None = 0.7


class MoviePublic(SQLModel):
    id: int
    tmdb_id: int
    title: str
    genres: list[Genre]
    production_companies: list[ProductionCompany]
    production_countries: list[ProductionCountry]
    keywords: list[Keyword]
    vote_average: float | None = None
    release_date: date | None = None
    overview: str | None = None
    poster_path: str | None = None
    runtime: int | None = None
