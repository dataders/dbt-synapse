from dataclasses import dataclass, field

from dbt.adapters.base.relation import Policy
from dbt.adapters.fabric.fabric_relation import FabricRelation


@dataclass
class SynapseIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SynapseRelation(FabricRelation):
    include_policy: SynapseIncludePolicy = field(default_factory=lambda: SynapseIncludePolicy())
