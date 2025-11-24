import ast
import re


def parse_bool(val: str):
    if val is None:
        return None
    val = val.strip()
    if val == "":
        return None
    lowered = val.lower()
    if lowered in {"true", "1", "t", "yes", "y"}:
        return True
    if lowered in {"false", "0", "f", "no", "n"}:
        return False
    # fallback: if it's numeric non-zero -> True
    try:
        return bool(int(val))
    except Exception:
        return None


def parse_int(val: str):
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except Exception:
        return None


def parse_float(val: str):
    if val is None or val == "":
        return None
    try:
        return float(val)
    except Exception:
        return None


def parse_list_field(value: str | None) -> list[str]:
    """
    Normalize genres, keywords, companies, etc.
    Accepts values like:
        "['Action','Drama']"
        "Action, Drama"
        "Action"
    """
    if not value:
        return []

    value = value.strip()

    # Try Python literal parsing first
    if value.startswith("[") and value.endswith("]"):
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass

    # Fallback: comma-separated
    return [v.strip() for v in value.split(",") if v.strip()]


def custom_slugify(s):
    s = s.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)  # Remove non-word chars (except spaces and hyphens)
    s = re.sub(
        r"[\s_-]+", "-", s
    )  # Replace spaces, underscores, and multiple hyphens with single hyphens
    s = re.sub(r"^-+|-+$", "", s)  # Remove leading/trailing hyphens
    return s


def get_sql_statement():
    return """
    SELECT
        m.id,
        m.tmdb_id,
        m.title,
        m.original_title,
        m.overview,
        m.release_date,
        m.runtime,
        m.vote_average,
        m.vote_count,
        m.popularity,
        m.poster_path,

        -- aggregated genres
        (
            SELECT GROUP_CONCAT(g.name, ', ')
            FROM genre g
            JOIN moviegenrelink l ON l.genre_id = g.id
            WHERE l.movie_id = m.id
        ) AS genres,

        -- aggregated keywords
        (
            SELECT GROUP_CONCAT(k.name, ', ')
            FROM keyword k
            JOIN moviekeywordlink l ON l.keyword_id = k.id
            WHERE l.movie_id = m.id
        ) AS keywords,

        -- aggregated production companies
        (
            SELECT GROUP_CONCAT(pc.name, ', ')
            FROM production_company pc
            JOIN movieproductioncompanylink l ON l.production_company_id = pc.id
            WHERE l.movie_id = m.id
        ) AS production_companies,

        -- aggregated countries
        (
            SELECT GROUP_CONCAT(pc.name, ', ')
            FROM production_country pc
            JOIN movieproductioncountrylink l ON l.production_country_id = pc.id
            WHERE l.movie_id = m.id
        ) AS production_countries

    FROM movie m
    """
