from typing import Annotated
from fastapi import APIRouter, Query
from sqlmodel import select, or_
from app.dependencies.db import SessionDep
from app.dependencies.recommender import RecommenderDep
from app.models.movie import Movie, MoviePublic, RecommendMoviesRequest
from fastapi.concurrency import run_in_threadpool


router = APIRouter()


@router.get("/search")
def get_movies(
    session: SessionDep,
    search: Annotated[str, Query(description="Search term for title or keywords")],
) -> list[MoviePublic]:
    """
    Search movies by title or keywords
    """
    if not search:
        return []

    term = f"%{search.lower()}%"
    stmt = (
        select(Movie)
        .where(
            or_(
                Movie.title.ilike(term),
                Movie.keywords.ilike(term),
            )
        )
        .limit(50)
    )

    results: list[Movie] = session.exec(stmt).all()
    return [m.to_public() for m in results]


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
