from datetime import date
from typing import Optional, List
from sqlmodel import Relationship, SQLModel, Field


# ---------------------------
# Link Tables
# ---------------------------


class MovieGenreLink(SQLModel, table=True):
    movie_id: int | None = Field(default=None, foreign_key="movie.id", primary_key=True)
    genre_id: int | None = Field(default=None, foreign_key="genre.id", primary_key=True)


class MovieProductionCompanyLink(SQLModel, table=True):
    movie_id: int | None = Field(default=None, foreign_key="movie.id", primary_key=True)
    production_company_id: int | None = Field(
        default=None, foreign_key="productioncompany.id", primary_key=True
    )


class MovieProductionCountryLink(SQLModel, table=True):
    movie_id: int | None = Field(default=None, foreign_key="movie.id", primary_key=True)
    production_country_id: int | None = Field(
        default=None, foreign_key="productioncountry.id", primary_key=True
    )


class MovieKeywordLink(SQLModel, table=True):
    movie_id: int | None = Field(default=None, foreign_key="movie.id", primary_key=True)
    keyword_id: int | None = Field(
        default=None, foreign_key="keyword.id", primary_key=True
    )


# ---------------------------
# Normalized Tables
# ---------------------------


class Genre(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    slug: str = Field(unique=True)
    name: str

    movies: List["Movie"] = Relationship(
        back_populates="genres", link_model=MovieGenreLink
    )


class ProductionCompany(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    slug: str = Field(unique=True)
    name: str

    movies: List["Movie"] = Relationship(
        back_populates="production_companies", link_model=MovieProductionCompanyLink
    )


class ProductionCountry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    slug: str = Field(unique=True)
    name: str

    movies: List["Movie"] = Relationship(
        back_populates="production_countries", link_model=MovieProductionCountryLink
    )


class Keyword(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    slug: str = Field(unique=True)
    name: str

    movies: List["Movie"] = Relationship(
        back_populates="keywords", link_model=MovieKeywordLink
    )


# ---------------------------
# Movie Table
# ---------------------------


class Movie(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tmdb_id: Optional[int] = None
    title: Optional[str] = None
    vote_average: Optional[float] = None
    vote_count: Optional[int] = None
    status: Optional[str] = None
    release_date: Optional[date] = None
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

    genres: List[Genre] = Relationship(
        back_populates="movies", link_model=MovieGenreLink
    )

    production_companies: List[ProductionCompany] = Relationship(
        back_populates="movies", link_model=MovieProductionCompanyLink
    )

    production_countries: List[ProductionCountry] = Relationship(
        back_populates="movies", link_model=MovieProductionCountryLink
    )

    keywords: List[Keyword] = Relationship(
        back_populates="movies", link_model=MovieKeywordLink
    )
