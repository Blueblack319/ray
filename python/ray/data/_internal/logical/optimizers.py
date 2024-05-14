from typing import List

from ray.data._internal.logical.interfaces import (
    LogicalPlan,
    Optimizer,
    PhysicalPlan,
    Rule,
)
from ray.data._internal.logical.rules._user_provided_optimizer_rules import (
    add_user_provided_logical_rules,
    add_user_provided_physical_rules,
)
from ray.data._internal.logical.rules.inherit_target_max_block_size import (
    InheritTargetMaxBlockSizeRule,
)
from ray.data._internal.logical.rules.operator_fusion import OperatorFusionRule
from ray.data._internal.logical.rules.randomize_blocks import ReorderRandomizeBlocksRule
from ray.data._internal.logical.rules.set_read_parallelism import SetReadParallelismRule
from ray.data._internal.logical.rules.zero_copy_map_fusion import (
    EliminateBuildOutputBlocks,
)
'''Our Optimizing Rule'''
from ray.data._internal.logical.rules.reorder_map_transforms import ReorderMapTransformDescendingRule

from ray.data._internal.planner.planner import Planner

DEFAULT_LOGICAL_RULES = [
    ReorderRandomizeBlocksRule,
]

DEFAULT_PHYSICAL_RULES = [
    InheritTargetMaxBlockSizeRule,
    SetReadParallelismRule,
    OperatorFusionRule,
    EliminateBuildOutputBlocks,
    # My New Rule
    ReorderMapTransformDescendingRule
]


class LogicalOptimizer(Optimizer):
    """The optimizer for logical operators."""

    @property
    def rules(self) -> List[Rule]:
        rules = add_user_provided_logical_rules(DEFAULT_LOGICAL_RULES)
        return [rule_cls() for rule_cls in rules]


class PhysicalOptimizer(Optimizer):
    """The optimizer for physical operators."""

    @property
    def rules(self) -> List["Rule"]:
        rules = add_user_provided_physical_rules(DEFAULT_PHYSICAL_RULES)
        return [rule_cls() for rule_cls in rules]


def get_execution_plan(logical_plan: LogicalPlan) -> PhysicalPlan:
    """Get the physical execution plan for the provided logical plan.

    This process has 3 steps:
    (1) logical optimization: optimize logical operators.
    (2) planning: convert logical to physical operators.
    (3) physical optimization: optimize physical operators.
    """
    print(f"Logical Unoptimized DAG: {logical_plan.dag}\n")
    optimized_logical_plan = LogicalOptimizer().optimize(logical_plan)
    logical_plan._dag = optimized_logical_plan.dag
    print(f"Logical Optimized DAG: {logical_plan.dag}\n")
    physical_plan = Planner().plan(optimized_logical_plan)
    print(f"Physical Unoptimized DAG: {physical_plan.dag}\n")
    return PhysicalOptimizer().optimize(physical_plan)
