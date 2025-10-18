import json


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


def parse_list_field(val: str):
    """
    Convert a CSV field that represents a list into a python list.
    We will not try to parse complicated JSON-like strings — keep simple:
    - If empty -> []
    - If value contains ', ' -> split on ', '
    - Else if value contains ',' -> split on ',' (strip)
    - Else -> return [val] if not empty
    Return JSON string (to store in Movie.<field>).
    """
    if val is None:
        return None
    v = val.strip()
    if v == "":
        return json.dumps([])
    # If already appears as a JSON array (starts with [), try to parse it
    if v.startswith("[") and v.endswith("]"):
        try:
            parsed = json.loads(v)
            return json.dumps(parsed)
        except Exception:
            pass
    # split heuristics
    if ", " in v:
        items = [p.strip() for p in v.split(", ") if p.strip() != ""]
        return json.dumps(items)
    if "," in v:
        items = [p.strip() for p in v.split(",") if p.strip() != ""]
        return json.dumps(items)
    # single value
    return json.dumps([v]) if v != "" else json.dumps([])
