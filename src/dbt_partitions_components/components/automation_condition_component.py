"""Automation Condition Component with on_cron declarative scheduling.

Demonstrates Dagster's declarative automation using AutomationCondition.on_cron().
Instead of defining schedules/jobs imperatively, assets declare their own
materialization conditions. The Dagster daemon evaluates conditions and
triggers materializations automatically.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

import dagster as dg
from dagster.components import Component, ComponentLoadContext, Resolvable
from pydantic import BaseModel


class AutomatedAssetConfig(BaseModel):
    """Configuration for an asset with an automation condition."""

    name: str
    cron_schedule: str
    description: str = ""
    deps: list[str] = []
    group_name: str = "automated_analytics"
    timezone: str = "US/Eastern"


def _make_automated_asset(
    config: AutomatedAssetConfig,
    demo_mode: bool,
    project_root: str,
) -> dg.AssetsDefinition:
    """Factory: create an asset with an AutomationCondition.on_cron schedule."""

    dep_keys = [dg.AssetKey(d.split("/")) for d in config.deps]

    @dg.asset(
        key=dg.AssetKey(["analytics", config.name]),
        description=config.description or f"Automated analytics: {config.name}",
        deps=dep_keys,
        kinds={"python", "analytics"},
        group_name=config.group_name,
        automation_condition=dg.AutomationCondition.on_cron(
            config.cron_schedule, cron_timezone=config.timezone
        ),
        metadata={
            "cron_schedule": config.cron_schedule,
            "timezone": config.timezone,
            "automation_type": "on_cron",
            "demo_mode": str(demo_mode).lower(),
        },
        tags={"layer": "analytics", "automation": "on_cron"},
    )
    def automated_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        import json
        import os
        import random
        from datetime import datetime

        name = config.name
        context.log.info(
            f"{'[DEMO] ' if demo_mode else ''}Running automated analytics: {name}"
        )

        if name == "league_daily_summary":
            summary = _compute_league_summary(demo_mode, project_root, context)
        elif name == "player_watchlist_alerts":
            summary = _compute_watchlist_alerts(demo_mode, project_root, context)
        else:
            summary = {"asset": name, "computed_at": datetime.now().isoformat()}

        # Write output to JSON for auditability
        out_dir = os.path.join(project_root, "data", "analytics")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{name}.json")
        with open(out_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)

        context.log.info(f"Wrote analytics output to {out_path}")

        return dg.MaterializeResult(
            metadata={
                "output_path": out_path,
                "computed_at": datetime.now().isoformat(),
                "demo_mode": demo_mode,
                **{k: v for k, v in summary.items() if isinstance(v, (str, int, float, bool))},
            }
        )

    return automated_asset


def _compute_league_summary(
    demo_mode: bool, project_root: str, context: dg.AssetExecutionContext
) -> dict:
    """Compute a league-wide daily summary from report data."""
    import random
    from datetime import datetime

    if demo_mode:
        random.seed(hash(datetime.now().strftime("%Y-%m-%d")))
        context.log.info("[DEMO] Generating synthetic league summary")

        return {
            "report_date": datetime.now().strftime("%Y-%m-%d"),
            "games_played_yesterday": random.randint(4, 12),
            "avg_score": round(random.uniform(105, 115), 1),
            "highest_scorer": random.choice([
                "Luka Doncic", "Jayson Tatum", "Nikola Jokic",
                "Giannis Antetokounmpo", "Shai Gilgeous-Alexander",
            ]),
            "highest_score": random.randint(35, 55),
            "upsets": random.randint(0, 3),
            "total_three_pointers": random.randint(180, 350),
            "closest_game_margin": random.randint(1, 5),
            "overtime_games": random.randint(0, 2),
        }

    # Production: would query from dbt report tables
    context.log.info("Computing league summary from report tables")
    import duckdb
    import os

    db_path = os.path.join(project_root, "dbt_project", "nba_analytics.duckdb")
    conn = duckdb.connect(db_path, read_only=True)
    try:
        result = conn.execute(
            "SELECT count(*) as total_teams FROM nba_analytics.rpt_team_standings"
        ).fetchone()
        return {
            "report_date": datetime.now().strftime("%Y-%m-%d"),
            "total_teams": result[0] if result else 0,
        }
    except Exception as e:
        context.log.warning(f"Could not query report tables: {e}")
        return {"report_date": datetime.now().strftime("%Y-%m-%d"), "error": str(e)}
    finally:
        conn.close()


def _compute_watchlist_alerts(
    demo_mode: bool, project_root: str, context: dg.AssetExecutionContext
) -> dict:
    """Compute player watchlist alerts based on performance thresholds."""
    import random
    from datetime import datetime

    if demo_mode:
        random.seed(hash(datetime.now().strftime("%Y-%m-%d") + "watchlist"))
        context.log.info("[DEMO] Generating synthetic watchlist alerts")

        alert_types = ["breakout_game", "injury_risk", "trade_value_change", "slump_alert"]
        alerts = []
        for _ in range(random.randint(2, 6)):
            alerts.append({
                "player": random.choice([
                    "Marcus Johnson", "Tyler Chen", "Luis Rodriguez",
                    "Andre Thompson", "Chris Park", "DeAndre Williams",
                ]),
                "alert_type": random.choice(alert_types),
                "severity": random.choice(["low", "medium", "high"]),
                "detail": "Performance threshold exceeded",
            })

        return {
            "report_date": datetime.now().strftime("%Y-%m-%d"),
            "total_alerts": len(alerts),
            "high_severity_count": sum(1 for a in alerts if a["severity"] == "high"),
            "alerts": alerts,
        }

    context.log.info("Computing watchlist alerts from player performance data")
    return {
        "report_date": datetime.now().strftime("%Y-%m-%d"),
        "total_alerts": 0,
        "alerts": [],
    }


@dataclass
class AutomationConditionComponent(Component, Resolvable):
    """Creates assets with declarative automation conditions.

    Uses AutomationCondition.on_cron() to automatically materialize assets
    on a schedule without requiring explicit jobs or schedules. The Dagster
    daemon evaluates the condition and triggers runs when the cron fires.

    This is the declarative alternative to traditional ScheduleDefinition.
    Instead of "run this job on this schedule", assets declare "I should
    be materialized on this schedule" -- a subtle but powerful distinction
    that enables better composition and dependency-aware scheduling.

    Attributes:
        assets: List of automated asset configurations.
        demo_mode: If true, generates synthetic analytics data.
    """

    assets: Sequence[AutomatedAssetConfig] = field(default_factory=list)
    demo_mode: bool = False

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        project_root = str(Path(context.path).parents[3])

        built_assets = [
            _make_automated_asset(
                config=asset_config,
                demo_mode=self.demo_mode,
                project_root=project_root,
            )
            for asset_config in self.assets
        ]

        return dg.Definitions(assets=built_assets)
