from fastapi import APIRouter

from app.routes.movies import router as movies_router


router = APIRouter()
router.include_router(movies_router, prefix="/movies", tags=["movies"])
