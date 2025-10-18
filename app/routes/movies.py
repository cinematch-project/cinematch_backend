from fastapi import APIRouter, Query
from sqlmodel import select, or_
from app.dependencies.db import SessionDep
from app.models.movie import Movie, MoviePublic


router = APIRouter()


@router.get("/")
def get_movies(
    session: SessionDep,
    search: str | None = Query(None, description="Search term for title or keywords"),
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
