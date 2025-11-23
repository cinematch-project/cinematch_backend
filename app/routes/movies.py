from typing import Annotated
from fastapi import APIRouter, Query
from sqlmodel import select, or_
from app.dependencies.db import SessionDep
from app.dependencies.recommender import RecommenderDep
from app.models.dto import (
    FilterItem,
    FilterResponse,
    MoviePublic,
    MovieSearchResponse,
    RecommendMoviesRequest,
)
from app.models.movie import (
    Genre,
    Movie,
    MovieGenreLink,
    MovieKeywordLink,
    MovieProductionCompanyLink,
    MovieProductionCountryLink,
    ProductionCountry,
)
from fastapi.concurrency import run_in_threadpool

PER_PAGE = 25
TOTAL_COUNT = 915684

router = APIRouter()


@router.get("/search", response_model=MovieSearchResponse)
def get_movies(
    session: SessionDep,
    search: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    vote_average_from: float | None = None,
    vote_average_to: float | None = None,
    runtime_from: int | None = None,
    runtime_to: int | None = None,
    genres: list[int] = Query(default=[]),  # list of genre IDs
    companies: list[int] = Query(default=[]),  # list of company IDs
    countries: list[int] = Query(default=[]),  # list of country IDs
    keywords: list[int] = Query(default=[]),  # list of keyword IDs
    page: int = 1,
) -> list[MoviePublic]:
    """
    Search movies using multiple filters.
    Empty filters are ignored.
    """
    stmt = select(Movie)

    if search:
        term = f"%{search.lower()}%"
        stmt = stmt.where(
            or_(Movie.title.ilike(term), Movie.original_title.ilike(term))
        )

    if year_from:
        stmt = stmt.where(Movie.release_date >= f"{year_from}-01-01")
    if year_to:
        stmt = stmt.where(Movie.release_date <= f"{year_to}-12-31")

    if vote_average_from is not None:
        stmt = stmt.where(Movie.vote_average >= vote_average_from)
    if vote_average_to is not None:
        stmt = stmt.where(Movie.vote_average <= vote_average_to)

    if runtime_from is not None:
        stmt = stmt.where(Movie.runtime >= runtime_from)
    if runtime_to is not None:
        stmt = stmt.where(Movie.runtime <= runtime_to)

    for g in genres:
        stmt = stmt.where(
            Movie.id.in_(
                select(MovieGenreLink.movie_id).where(MovieGenreLink.genre_id == g)
            )
        )

    for c in companies:
        stmt = stmt.where(
            Movie.id.in_(
                select(MovieProductionCompanyLink.movie_id).where(
                    MovieProductionCompanyLink.production_company_id == c
                )
            )
        )

    for c in countries:
        stmt = stmt.where(
            Movie.id.in_(
                select(MovieProductionCountryLink.movie_id).where(
                    MovieProductionCountryLink.production_country_id == c
                )
            )
        )

    for k in keywords:
        stmt = stmt.where(
            Movie.id.in_(
                select(MovieKeywordLink.movie_id).where(
                    MovieKeywordLink.keyword_id == k
                )
            )
        )

    offset = (page - 1) * PER_PAGE
    stmt = stmt.offset(offset).limit(PER_PAGE)
    total_pages = (TOTAL_COUNT + PER_PAGE - 1) // PER_PAGE

    results: list[Movie] = session.exec(stmt).all()
    return MovieSearchResponse(items=results, totalPages=total_pages)


@router.get("/batch")
def get_movies_batch(
    session: SessionDep,
    ids: Annotated[list[int], Query(description="List of movie IDs")],
) -> list[MoviePublic]:
    """
    Get movies by ID
    """
    if not ids:
        return []

    ordered_ids = list(set(ids))

    stmt = select(Movie).where(Movie.id.in_(ordered_ids))
    results: list[Movie] = session.exec(stmt).all()

    by_id = {m.id: m for m in results}
    response: list[MoviePublic] = []
    for _id in ids:
        movie = by_id.get(_id)
        if movie:
            response.append(movie)
    return response


@router.post("/recommend")
async def recommend_by_ids(
    session: SessionDep,
    recommender: RecommenderDep,
    body: RecommendMoviesRequest,
) -> list[MoviePublic]:
    """
    Get recommended movies
    """
    # run recommendation in a threadpool (CPU-bound / pandas / numpy)
    df_recs = await run_in_threadpool(
        recommender.recommend, body.ids, body.top_n, body.similarity_weight
    )

    ids = [int(row["id"]) for _, row in df_recs.iterrows()]

    return get_movies_batch(session, ids)


@router.get("/filters", response_model=FilterResponse)
def get_filter_lists(session: SessionDep) -> FilterResponse:
    """
    Returns lists of:
      - all production companies
      - all production countries
    sorted alphabetically by name.
    """

    genres = session.exec(select(Genre).order_by(Genre.name)).all()
    countries = session.exec(
        select(ProductionCountry).order_by(ProductionCountry.name)
    ).all()

    return FilterResponse(
        genres=[FilterItem(id=g.id, name=g.name) for g in genres],
        countries=[FilterItem(id=c.id, name=c.name) for c in countries],
    )
