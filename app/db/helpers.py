import ast


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
