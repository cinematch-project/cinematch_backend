from typing import Annotated
from fastapi import APIRouter, Query
from sqlmodel import select, or_
from app.dependencies.db import SessionDep
from app.models.movie import Movie, MoviePublic


router = APIRouter()


@router.get("/search")
def get_movies(
    session: SessionDep,
    search: Annotated[str, Query(description="Search term for title or keywords")],
) -> list[MoviePublic]:
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
    ids: Annotated[list[int], Query(description="List of movie IDs")]
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