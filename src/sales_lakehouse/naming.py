"""Validated Unity Catalog names for one environment.

Catalog and environment names arrive as notebook/job parameters, so they are
validated here before they are ever placed in SQL. Only letters, digits and
underscores are accepted; names are lowercased because Unity Catalog stores
them in lowercase.
"""

import re
from dataclasses import dataclass

_IDENTIFIER = re.compile(r"[A-Za-z0-9_]+")

MAX_CATALOG_LENGTH = 64
MAX_ENV_LENGTH = 20

LAYERS = ("bronze", "silver", "gold")
RAW_VOLUME = "raw_data"


def validate_identifier(value, kind, max_length):
    """Return `value` lowercased, or raise ValueError if it is not a safe identifier."""
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(
            f"Invalid {kind} name {value!r}: use only letters, digits and underscores."
        )
    if len(value) > max_length:
        raise ValueError(
            f"Invalid {kind} name {value!r}: longer than {max_length} characters."
        )
    return value.lower()


def quote(*parts):
    """Backtick-quote already validated name parts, e.g. `cat`.`schema`."""
    return ".".join(f"`{part}`" for part in parts)


@dataclass(frozen=True)
class LayerNames:
    catalog: str
    env: str

    @property
    def bronze(self):
        return f"{self.env}_bronze"

    @property
    def silver(self):
        return f"{self.env}_silver"

    @property
    def gold(self):
        return f"{self.env}_gold"

    @property
    def schemas(self):
        return (self.bronze, self.silver, self.gold)

    @property
    def raw_volume(self):
        return RAW_VOLUME

    @property
    def raw_volume_path(self):
        return f"/Volumes/{self.catalog}/{self.bronze}/{self.raw_volume}"


def layer_names(catalog, env):
    """Validate the parameters and return the names for that environment."""
    return LayerNames(
        catalog=validate_identifier(catalog, "catalog", MAX_CATALOG_LENGTH),
        env=validate_identifier(env, "environment", MAX_ENV_LENGTH),
    )
