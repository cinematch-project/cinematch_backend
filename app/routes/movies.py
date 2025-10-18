from fastapi import APIRouter, Query
from sqlmodel import select, or_
from app.dependencies.db import SessionDep
from app.models.movie import Movie


router = APIRouter()


@router.get("/", response_model=list[Movie])
def get_movies(
    session: SessionDep,
    search: str | None = Query(None, description="Search term for title or keywords"),
) -> list[Movie]:
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

    results = session.exec(stmt).all()
    return results
