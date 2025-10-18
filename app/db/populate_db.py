from pathlib import Path
import csv
from sqlmodel import Session
from app.db.helpers import parse_bool, parse_float, parse_int, parse_list_field
from app.models.movie import Movie


def populate_from_csv(engine, csv_path: str):
    """
    Read CSV and populate Movie table.
    Uses chunked inserts for memory safety.
    """
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")

    BATCH_SIZE = 500
    current_batch = 0
    movies_batch = []

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row_idx, row in enumerate(reader, start=1):
            try:
                m = Movie(
                    tmdb_id=parse_int(row.get("id")),
                    title=row.get("title") or None,
                    vote_average=parse_float(row.get("vote_average")),
                    vote_count=parse_int(row.get("vote_count")),
                    status=row.get("status") or None,
                    release_date=row.get("release_date") or None,
                    revenue=parse_int(row.get("revenue")),
                    runtime=parse_int(row.get("runtime")),
                    adult=parse_bool(row.get("adult")),
                    backdrop_path=row.get("backdrop_path") or None,
                    budget=parse_int(row.get("budget")),
                    homepage=row.get("homepage") or None,
                    imdb_id=row.get("imdb_id") or None,
                    original_language=row.get("original_language") or None,
                    original_title=row.get("original_title") or None,
                    overview=row.get("overview") or None,
                    popularity=parse_float(row.get("popularity")),
                    poster_path=row.get("poster_path") or None,
                    tagline=row.get("tagline") or None,
                    genres=parse_list_field(row.get("genres")),
                    production_companies=parse_list_field(
                        row.get("production_companies")
                    ),
                    production_countries=parse_list_field(
                        row.get("production_countries")
                    ),
                    spoken_languages=parse_list_field(row.get("spoken_languages")),
                    keywords=parse_list_field(row.get("keywords")),
                )
            except Exception as e:
                # Skip bad rows but log them (here we simply print)
                print(f"Error parsing row {row_idx}: {e}. Row data: {row}")
                continue

            movies_batch.append(m)

            if len(movies_batch) >= BATCH_SIZE:
                with Session(engine) as session:
                    session.add_all(movies_batch)
                    session.commit()
                movies_batch = []
                current_batch += 1
                print(f"Populated batch: {current_batch}")

    # insert remaining
    if movies_batch:
        with Session(engine) as session:
            session.add_all(movies_batch)
            session.commit()
