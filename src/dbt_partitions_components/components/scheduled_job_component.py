"""Scheduled Job Component for flexible asset scheduling.

Creates Dagster jobs and schedules using asset selection syntax,
enabling scheduling by tags, groups, kinds, or specific keys.
"""

from dataclasses import dataclass, field
from typing import Sequence

import dagster as dg
from dagster.components import Component, ComponentLoadContext, Resolvable
from pydantic import BaseModel


class ScheduledJobConfig(BaseModel):
    """Configuration for a single scheduled job."""

    name: str
    cron_schedule: str
    asset_selection: str
    description: str = ""
    default_status: str = "RUNNING"
    execution_timezone: str = "US/Eastern"


@dataclass
class ScheduledJobComponent(Component, Resolvable):
    """Creates scheduled jobs for asset materializations.

    Uses Dagster's asset selection syntax to target assets by tags,
    groups, kinds, or specific keys. This provides flexible scheduling
    without hardcoding asset references.

    Attributes:
        jobs: List of scheduled job configurations.
    """

    jobs: Sequence[ScheduledJobConfig] = field(default_factory=list)

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        schedules = []
        jobs = []

        for job_config in self.jobs:
            selection = dg.AssetSelection.from_string(job_config.asset_selection)

            job = dg.define_asset_job(
                name=job_config.name,
                selection=selection,
                description=job_config.description,
            )
            jobs.append(job)

            schedule = dg.ScheduleDefinition(
                name=f"{job_config.name}_schedule",
                job=job,
                cron_schedule=job_config.cron_schedule,
                default_status=(
                    dg.DefaultScheduleStatus.RUNNING
                    if job_config.default_status == "RUNNING"
                    else dg.DefaultScheduleStatus.STOPPED
                ),
                execution_timezone=job_config.execution_timezone,
            )
            schedules.append(schedule)

        return dg.Definitions(jobs=jobs, schedules=schedules)
