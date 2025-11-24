from datetime import datetime
from pathlib import Path
import csv
from sqlmodel import Session
from app.db.helpers import (
    custom_slugify,
    parse_bool,
    parse_float,
    parse_int,
    parse_list_field,
)
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

    current_batch = 0
    ids = set()
    genre_cache = {}
    company_cache = {}
    country_cache = {}
    keyword_cache = {}

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row_idx, row in enumerate(reader, start=1):
            tmdb_id = parse_int(row.get("id"))
            if tmdb_id in ids:
                continue
            movie = Movie(
                tmdb_id=tmdb_id,
                title=row.get("title") or None,
                vote_average=parse_float(row.get("vote_average")),
                vote_count=parse_int(row.get("vote_count")),
                status=row.get("status") or None,
                release_date=(
                    datetime.strptime(row["release_date"], "%Y-%m-%d").date()
                    if row.get("release_date")
                    else None
                ),
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
            movie_genres = set()
            for name in parse_list_field(row.get("genres")):
                slug = custom_slugify(name)
                genre = genre_cache.get(slug)

                if not genre:
                    genre = Genre(slug=slug, name=name)
                    session.add(genre)
                    session.flush()

                if slug not in movie_genres:
                    session.add(MovieGenreLink(movie_id=movie.id, genre_id=genre.id))
                    movie_genres.add(slug)

                genre_cache[slug] = genre

            # ------------------------
            # Normalize Production Companies
            # ------------------------
            movie_companies = set()
            for name in parse_list_field(row.get("production_companies")):
                slug = custom_slugify(name)
                company = company_cache.get(slug)

                if not company:
                    company = ProductionCompany(slug=slug, name=name)
                    session.add(company)
                    session.flush()

                if slug not in movie_companies:
                    session.add(
                        MovieProductionCompanyLink(
                            movie_id=movie.id,
                            production_company_id=company.id,
                        )
                    )
                    movie_companies.add(slug)

                company_cache[slug] = company

            # ------------------------
            # Normalize Production Countries
            # ------------------------
            movie_countries = set()
            for name in parse_list_field(row.get("production_countries")):
                slug = custom_slugify(name)
                country = country_cache.get(slug)

                if not country:
                    country = ProductionCountry(slug=slug, name=name)
                    session.add(country)
                    session.flush()

                if slug not in movie_countries:
                    session.add(
                        MovieProductionCountryLink(
                            movie_id=movie.id,
                            production_country_id=country.id,
                        )
                    )
                    movie_countries.add(slug)

                country_cache[slug] = country

            # ------------------------
            # Normalize Keywords
            # ------------------------
            movie_keywords = set()
            for name in parse_list_field(row.get("keywords")):
                slug = custom_slugify(name)
                keyword = keyword_cache.get(slug)

                if not keyword:
                    keyword = Keyword(slug=slug, name=name)
                    session.add(keyword)
                    session.flush()

                if slug not in movie_keywords:
                    session.add(
                        MovieKeywordLink(movie_id=movie.id, keyword_id=keyword.id)
                    )
                    movie_keywords.add(slug)

                keyword_cache[slug] = keyword

            if row_idx % 500 == 0:
                session.commit()
                current_batch += 1
                if current_batch % 10 == 0:
                    print(f"Populated batch: {current_batch}")

    session.commit()
    print("Database populated (normalized)!")
