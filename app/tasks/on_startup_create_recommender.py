from pathlib import Path

from app.services.MovieRecommender import MovieRecommender

BASE_DIR = Path(__file__).resolve().parent.parent

TARGET_DB_VERSION = 1
MODEL_PATH = BASE_DIR / "data" / "recommender.joblib"


def on_startup_create_recommender(engine) -> MovieRecommender:
    if MODEL_PATH.exists():
        print("Loading recommender from saved state...")
        recommender = MovieRecommender.load(MODEL_PATH)
        return recommender

    print("Building recommender from database...")
    recommender = MovieRecommender(engine=engine)
    recommender.save(MODEL_PATH)
    return recommender
