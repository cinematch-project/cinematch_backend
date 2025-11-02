from fastapi import Depends, HTTPException, Request
from typing import Annotated
from app.services.MovieRecommender import MovieRecommender


def get_recommender(request: Request) -> MovieRecommender:
    """
    FastAPI dependency that retrieves the recommender from app.state.
    Raises 503 if not available.
    """
    rec = getattr(request.app.state, "recommender", None)
    if rec is None:
        raise HTTPException(status_code=503, detail="Recommender not loaded")
    return rec


RecommenderDep = Annotated[MovieRecommender, Depends(get_recommender)]
