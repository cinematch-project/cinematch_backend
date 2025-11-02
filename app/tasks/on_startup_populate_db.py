from app.db.db import get_engine
from pathlib import Path
from app.db.populate_db import populate_from_csv
from app.models.db_version import DBVersion
from datetime import datetime
from sqlmodel import SQLModel, Session, select

BASE_DIR = Path(__file__).resolve().parent.parent

TARGET_DB_VERSION = 1
DATA_DIR = BASE_DIR / "data"
DB_FILE = DATA_DIR / "movies.db"
CSV_FILE = DATA_DIR / "TMDB_movie_dataset_v11.csv"


def on_startup_populate_db(db_file: str = DB_FILE, csv_file: str = CSV_FILE):
    """
    On startup: create or migrate DB.
    - If DB file missing OR stored version != TARGET_DB_VERSION:
        - (re)create tables
        - populate Movie table from CSV
        - set DBVersion.version = TARGET_DB_VERSION
    """
    engine = get_engine()

    db_needs_setup = False

    if not DATA_DIR.exists():
        DATA_DIR.mkdir(parents=True)

    # 1) If DB file doesn't exist -> must setup
    if not Path(db_file).exists():
        print("DB file not found -> creating new DB and populating from CSV.")
        db_needs_setup = True
    else:
        # 2) DB exists. Try to read DBVersion table
        try:
            # Ensure metadata exists so DBVersion class known to ORM introspection
            # Try opening a session and querying db_version
            with Session(engine) as session:
                # if DBVersion table doesn't exist, the below query will raise
                res = session.exec(select(DBVersion)).all()
                if not res:
                    # either table exists but empty -> treat as wrong version
                    print(
                        "DBVersion table empty or not present -> reinitialization required."
                    )
                    db_needs_setup = True
                else:
                    current_version = res[0].version
                    print(f"Found existing DB version: {current_version}")
                    if current_version != TARGET_DB_VERSION:
                        print(
                            f"DB version {current_version} != target {TARGET_DB_VERSION} -> reinitialization required."
                        )
                        db_needs_setup = True
        except Exception as exc:
            # Anything goes wrong (table missing, schema mismatch) -> reinit
            print(
                "Error checking DB version (likely missing table). Will reinitialize DB. Error:",
                exc,
            )
            db_needs_setup = True

    if db_needs_setup:
        # If tables exist and we are reinitializing, drop all to ensure clean schema
        try:
            print("Dropping existing tables (if any) and creating fresh schema...")
            SQLModel.metadata.drop_all(engine)
        except Exception as e:
            print("Warning: error while dropping tables (may be first run):", e)

        # Create tables
        SQLModel.metadata.create_all(engine)
        print("Created all tables.")

        # Populate from CSV
        try:
            print("Populating Movie table from CSV:", csv_file)
            populate_from_csv(engine, csv_file)
            print("Population finished.")
        except Exception as e:
            print("Error populating DB from CSV:", e)
            raise

        # Insert DBVersion row
        with Session(engine) as session:
            dbv = DBVersion(version=TARGET_DB_VERSION, updated_at=datetime.utcnow())
            session.add(dbv)
            session.commit()
            print(f"Set DB version to {TARGET_DB_VERSION}.")

    return engine
