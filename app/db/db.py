from pathlib import Path
from sqlmodel import Session, create_engine


BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR.parent / "data" / "movies.db"
DATABASE_URL = f"sqlite:///{DB_FILE}"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})


def get_engine():
    return engine


def get_session():
    with Session(engine) as session:
        yield session
