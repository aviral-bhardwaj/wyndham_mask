"""Authorization policy is installed by an operator, never supplied by a request."""
from dataclasses import dataclass
import getpass


@dataclass(frozen=True)
class Identity:
    requester: str
    executor: str


class LocalIdentity:
    """OS account identity for local development. Not a remote authentication adapter."""
    def current(self):
        return Identity(getpass.getuser(), getpass.getuser())


class DatabricksIdentity:
    def __init__(self, spark):
        self.spark = spark

    def current(self):
        principal = self.spark.sql("SELECT session_user() AS principal").first().principal
        # A Job runs as its configured principal. This does not impersonate its triggerer.
        return Identity(principal, principal)


class PermissionManager:
    def __init__(self, identity_provider, grants):
        self.identity_provider = identity_provider
        self.grants = tuple(grants)

    def identity(self):
        return self.identity_provider.current()

    def require(self, identity, action, scope, table=None, column=None, destination=None):
        for grant in self.grants:
            if identity.requester not in grant.get("principals", []):
                continue
            if action not in grant.get("actions", []):
                continue
            if grant.get("namespace") != scope.namespace or scope.domain not in grant.get("domains", []):
                continue
            # Explicit wildcard is allowed for table/column grants only.
            if table is not None and table not in grant.get("tables", []) and "*" not in grant.get("tables", []):
                continue
            if column is not None and column not in grant.get("columns", []) and "*" not in grant.get("columns", []):
                continue
            if table is None and not grant.get("allow_values", False):
                continue
            if destination is not None and destination not in grant.get("destinations", []):
                continue
            return
        raise PermissionError("The execution identity is not authorized for this operation")
