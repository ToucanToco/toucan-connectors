def slugify(name: str, separator: str = "-") -> str:
    """Returns a slugified name (we allow _ to be used)"""
    from slugify import slugify as _slugify

    return _slugify(name, regex_pattern="[^-_a-z0-9]+", separator=separator)
