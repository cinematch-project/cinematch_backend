from pathlib import Path
import gdown
from app.services.MovieRecommender import MovieRecommender

BASE_DIR = Path(__file__).resolve().parent.parent

TARGET_DB_VERSION = 1
MODEL_PATH = BASE_DIR / "data" / "recommender.joblib"
MODEL_URL = "https://drive.google.com/uc?id=1A4VHJx5YxvEyepuQMSK_9cYDDRY-pI8r"


def download_model():
    print("Downloading recommender model...")
    gdown.download(MODEL_URL, str(MODEL_PATH))


def on_startup_create_recommender(engine) -> MovieRecommender:
    if not MODEL_PATH.exists() and MODEL_URL:
        download_model()

    if MODEL_PATH.exists():
        recommender = MovieRecommender.load(MODEL_PATH)
        return recommender

    recommender = MovieRecommender(engine=engine)
    recommender.save(MODEL_PATH)
    return recommender
