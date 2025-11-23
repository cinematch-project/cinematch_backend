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

    total = session.exec(stmt.with_only_columns(Movie.id).order_by(None)).all()

    total_count = len(total)
    total_pages = (total_count + PER_PAGE - 1) // PER_PAGE

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
            response.append(movie.to_public())
    return response


@router.post("/recommend")
async def recommend_by_ids(
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

    # map DataFrame rows to MoviePublic (parse genres as needed)
    results = []
    for _, row in df_recs.iterrows():
        genres = []
        val = row.get("genres")
        if isinstance(val, str):
            import ast

            try:
                parsed = ast.literal_eval(val)
                if isinstance(parsed, (list, tuple)):
                    genres = [str(g).strip().strip('"').strip("'") for g in parsed if g]
                else:
                    genres = [
                        s.strip().strip('"').strip("'")
                        for s in str(val).split(",")
                        if s.strip()
                    ]
            except Exception:
                genres = [
                    s.strip().strip('"').strip("'")
                    for s in str(val).split(",")
                    if s.strip()
                ]
        elif isinstance(val, list):
            genres = val

        results.append(
            MoviePublic(
                id=int(row.get("id")),
                tmdb_id=int(row.get("tmdb_id")),
                title=row.get("title"),
                vote_average=(
                    float(row.get("vote_average"))
                    if row.get("vote_average") is not None
                    else None
                ),
                release_date=row.get("release_date"),
                overview=row.get("overview"),
                poster_path=(
                    row.get("poster_path") if "poster_path" in row.index else None
                ),
                genres=genres,
            )
        )

    return results


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
