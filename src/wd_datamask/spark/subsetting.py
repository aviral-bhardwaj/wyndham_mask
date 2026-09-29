"""Explicit one-parent/one-child subsetting; caller composes multi-level subsets."""
from ..exceptions import IntegrityError


def subset_related(parent, child, *, parent_key, child_key, predicate):
    from pyspark.sql import functions as F
    from .dataframe_masker import quoted
    parents = parent.where(predicate)
    keys = parent.select(F.col(quoted(parent_key)).alias("_parent_key"))
    if keys.where(F.col("_parent_key").isNull()).limit(1).count() or keys.groupBy("_parent_key").count().where("count > 1").limit(1).count():
        raise IntegrityError("Parent keys must be unique and non-null")
    non_null = child.where(F.col(quoted(child_key)).isNotNull())
    if non_null.join(keys, non_null[child_key] == keys["_parent_key"], "left_anti").limit(1).count():
        raise IntegrityError("Source child data contains orphan foreign keys")
    selected = parents.select(F.col(quoted(parent_key)).alias("_parent_key"))
    children = child.join(selected, child[child_key] == selected["_parent_key"], "left_semi")
    return parents, children
