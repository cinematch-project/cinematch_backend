from datetime import datetime
from pathlib import Path
import csv
import slugify
from sqlmodel import Session
from app.db.helpers import parse_bool, parse_float, parse_int, parse_list_field
from app.models.movie import (
    Genre,
    Keyword,
    Movie,
    MovieGenreLink,
    MovieKeywordLink,
    MovieProductionCompanyLink,
    MovieProductionCountryLink,
    ProductionCompany,
    ProductionCountry,
)


def populate_from_csv(session: Session, csv_path: str):
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")

    current = 0
    ids = set()
    genre_cache = {}
    company_cache = {}
    country_cache = {}
    keyword_cache = {}

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            current += 1
            tmdb_id = parse_int(row.get("id"))
            if tmdb_id in ids:
                continue
            movie = Movie(
                tmdb_id=tmdb_id,
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
            )
            ids.add(tmdb_id)

            session.add(movie)
            session.flush()

            # ------------------------
            # Normalize Genres
            # ------------------------
            for name in parse_list_field(row.get("genres")):
                slug = slugify(name)
                genre = genre_cache.get(slug)

                if not genre:
                    genre = Genre(slug=slug, name=name)
                    session.add(genre)
                    session.flush()

                genre_cache[slug] = genre
                session.add(MovieGenreLink(movie_id=movie.id, genre_id=genre.id))

            # ------------------------
            # Normalize Production Companies
            # ------------------------
            for name in parse_list_field(row.get("production_companies")):
                slug = slugify(name)
                company = company_cache.get(slug)

                if not company:
                    company = ProductionCompany(slug=slug, name=name)
                    session.add(company)
                    session.flush()

                company_cache[slug] = company
                session.add(
                    MovieProductionCompanyLink(
                        movie_id=movie.id,
                        production_company_id=company.id,
                    )
                )

            # ------------------------
            # Normalize Production Countries
            # ------------------------
            for name in parse_list_field(row.get("production_countries")):
                slug = slugify(name)
                country = country_cache.get(slug)

                if not country:
                    country = ProductionCountry(slug=slug, name=name)
                    session.add(country)
                    session.flush()

                country_cache[slug] = country
                session.add(
                    MovieProductionCountryLink(
                        movie_id=movie.id,
                        production_country_id=country.id,
                    )
                )

            # ------------------------
            # Normalize Keywords
            # ------------------------
            for name in parse_list_field(row.get("keywords")):
                slug = slugify(name)
                keyword = keyword_cache.get(slug)

                if not keyword:
                    keyword = Keyword(slug=slug, name=name)
                    session.add(keyword)
                    session.flush()

                keyword_cache[slug] = keyword
                session.add(MovieKeywordLink(movie_id=movie.id, keyword_id=keyword.id))

        current += 1
        if current % 50000 == 0:
            print(f"Processed {current} movies...")

    session.commit()
    print("Database populated (normalized)!")
