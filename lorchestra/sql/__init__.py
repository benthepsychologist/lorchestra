"""SQL modules for lorchestra projections."""

from lorchestra.sql.pm_projections import PM_PROJECTIONS
from lorchestra.sql.projections import PROJECTIONS, get_projection_sql

# Merge PM projections into the main registry so get_projection_sql() covers them
PROJECTIONS.update(PM_PROJECTIONS)

__all__ = ["PROJECTIONS", "PM_PROJECTIONS", "get_projection_sql"]
