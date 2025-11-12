import re
import unicodedata


def safe_filename(name: str, default_ext: str = ".html") -> str:
    """Return a Unicode-normalized, filesystem-safe filename with a valid extension."""
    name = unicodedata.normalize("NFKC", name).strip()
    name = re.sub(r"[\\/]+", "_", name)
    name = re.sub(r"\s+", " ", name)
    name = "".join(c for c in name if c.isalnum() or c in "._- ")
    name = name.rstrip(". ")
    if not name.lower().endswith(default_ext):
        name += default_ext
    return name
