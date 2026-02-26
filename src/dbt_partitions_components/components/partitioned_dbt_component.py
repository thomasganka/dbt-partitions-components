"""Partitioned dbt Project Component.

Extends the DbtProjectComponent to add daily partition support,
enabling efficient backfills of dbt models across historical date ranges.
This demonstrates how to subclass an existing Dagster component to add
custom behavior like partitioning and concurrency control.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Mapping, Sequence

import dagster as dg
from dagster.components import Component, ComponentLoadContext, Resolvable, Resolver


@dataclass
class PartitionedDbtComponent(Component, Resolvable):
    """A dbt project component with daily partition support for backfills.

    Wraps a dbt project and adds daily partitions to all models, enabling:
    - Incremental backfills across historical date ranges
    - Concurrency-controlled partition processing
    - Partition-aware dbt variable injection (execution_date)

    The partition date is passed to dbt as a variable, allowing models
    to filter data using `{{ var('execution_date') }}` in their SQL.

    Attributes:
        dbt_project_dir: Path to the dbt project directory (relative to project root).
        partition_start_date: Start date for the daily partition range.
        max_partitions_per_run: Max partitions to process in a single backfill run.
        demo_mode: If true, uses DuckDB profiles for local execution.
        dbt_vars: Additional dbt variables to pass during execution.
    """

    dbt_project_dir: str = "dbt_project"
    partition_start_date: str = "2024-10-22"
    max_partitions_per_run: int = 10
    demo_mode: bool = False
    dbt_vars: Mapping[str, str] = field(default_factory=dict)

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

        # Walk up from the component defs directory to find the project root
        # context.path = .../src/dbt_partitions_components/defs/nba_dbt_models
        # project root = .../dbt-partitions-components (4 levels up)
        project_root = Path(context.path).parents[3]
        project_dir = project_root / self.dbt_project_dir
        dbt_project = DbtProject(project_dir=str(project_dir))

        daily_partitions = dg.DailyPartitionsDefinition(
            start_date=self.partition_start_date,
        )

        if self.demo_mode:
            dbt_project.prepare_if_dev()

        manifest_path = dbt_project.manifest_path

        @dbt_assets(
            manifest=manifest_path,
            project=dbt_project,
            partitions_def=daily_partitions,
            backfill_policy=dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.max_partitions_per_run
            ),
        )
        def nba_dbt_models(
            context: dg.AssetExecutionContext,
            dbt: DbtCliResource,
        ):
            partition_date = context.partition_key
            dbt_vars = {
                "execution_date": partition_date,
                **dict(self.dbt_vars),
            }
            yield from dbt.cli(
                ["build", "--vars", str(dbt_vars)],
                context=context,
            ).stream()

        dbt_resource = DbtCliResource(
            project_dir=str(project_dir),
            profiles_dir=str(project_dir) if self.demo_mode else None,
            global_config_flags=["--no-use-colors"],
        )

        # Set DBT_PROJECT_ROOT so the on-run-start macro can find Parquet files
        import os

        os.environ["DBT_PROJECT_ROOT"] = str(project_root)

        return dg.Definitions(
            assets=[nba_dbt_models],
            resources={"dbt": dbt_resource},
        )
