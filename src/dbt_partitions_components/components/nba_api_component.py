"""NBA API Data Ingestion Component with daily partitions.

Fetches game data, player stats, and team stats from the NBA API
(balldontlie.io) and stages them in DuckDB for downstream dbt processing.
Supports daily partitions for efficient backfills with concurrency control.
"""

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import dagster as dg
from dagster.components import Component, ComponentLoadContext, Resolvable
from pydantic import BaseModel


class NbaApiTableConfig(BaseModel):
    """Configuration for a single NBA API table to ingest."""

    name: str
    endpoint: str
    description: str = ""
    expected_row_count: int = 100


NBA_TEAMS = [
    ("BOS", "Boston Celtics"),
    ("MIL", "Milwaukee Bucks"),
    ("PHI", "Philadelphia 76ers"),
    ("CLE", "Cleveland Cavaliers"),
    ("NYK", "New York Knicks"),
    ("MIA", "Miami Heat"),
    ("ATL", "Atlanta Hawks"),
    ("CHI", "Chicago Bulls"),
    ("TOR", "Toronto Raptors"),
    ("BKN", "Brooklyn Nets"),
    ("DEN", "Denver Nuggets"),
    ("MEM", "Memphis Grizzlies"),
    ("SAC", "Sacramento Kings"),
    ("PHX", "Phoenix Suns"),
    ("LAL", "Los Angeles Lakers"),
    ("LAC", "LA Clippers"),
    ("GSW", "Golden State Warriors"),
    ("DAL", "Dallas Mavericks"),
    ("MIN", "Minnesota Timberwolves"),
    ("OKC", "Oklahoma City Thunder"),
]


def _generate_demo_data(
    table_name: str, partition_date: str, expected_rows: int
) -> list[dict]:
    """Generate realistic NBA demo data based on table type."""
    import random

    random.seed(hash(f"{table_name}_{partition_date}"))

    if table_name == "games":
        num_games = random.randint(3, 8)
        games = []
        used_teams: list[str] = []
        for i in range(num_games):
            available = [t for t in NBA_TEAMS if t[0] not in used_teams]
            if len(available) < 2:
                break
            home = random.choice(available)
            used_teams.append(home[0])
            available = [t for t in available if t[0] != home[0]]
            away = random.choice(available)
            used_teams.append(away[0])

            home_score = random.randint(85, 130)
            away_score = random.randint(85, 130)
            games.append(
                {
                    "id": i + 1000,
                    "date": partition_date,
                    "season": 2024,
                    "status": "Final",
                    "home_team": {"abbreviation": home[0], "full_name": home[1]},
                    "visitor_team": {"abbreviation": away[0], "full_name": away[1]},
                    "home_team_score": home_score,
                    "visitor_team_score": away_score,
                }
            )
        return games

    elif table_name == "player_stats":
        num_stats = random.randint(40, 80)
        stats = []
        for i in range(num_stats):
            team = random.choice(NBA_TEAMS)
            stats.append(
                {
                    "id": i + 5000,
                    "game_id": random.randint(1000, 1010),
                    "player_id": random.randint(1, 500),
                    "team": {"abbreviation": team[0]},
                    "date": partition_date,
                    "min": f"{random.randint(5, 40)}:00",
                    "pts": random.randint(0, 45),
                    "reb": random.randint(0, 18),
                    "ast": random.randint(0, 15),
                    "stl": random.randint(0, 5),
                    "blk": random.randint(0, 5),
                    "turnover": random.randint(0, 7),
                    "fgm": random.randint(0, 18),
                    "fga": random.randint(2, 25),
                    "fg3m": random.randint(0, 8),
                    "fg3a": random.randint(0, 12),
                    "ftm": random.randint(0, 12),
                    "fta": random.randint(0, 14),
                    "pf": random.randint(0, 6),
                }
            )
        return stats

    elif table_name == "team_stats":
        stats = []
        for team_abbr, team_name in NBA_TEAMS:
            stats.append(
                {
                    "team": {"abbreviation": team_abbr, "full_name": team_name},
                    "date": partition_date,
                    "wins": random.randint(0, 60),
                    "losses": random.randint(0, 60),
                    "pts_per_game": round(random.uniform(100, 120), 1),
                    "reb_per_game": round(random.uniform(38, 50), 1),
                    "ast_per_game": round(random.uniform(20, 30), 1),
                    "opp_pts_per_game": round(random.uniform(100, 120), 1),
                    "fg_pct": round(random.uniform(0.42, 0.52), 3),
                    "fg3_pct": round(random.uniform(0.32, 0.40), 3),
                    "ft_pct": round(random.uniform(0.72, 0.85), 3),
                }
            )
        return stats

    return [{"id": i, "date": partition_date} for i in range(expected_rows)]


def _make_production_asset(
    table_config: NbaApiTableConfig,
    daily_partitions: dg.DailyPartitionsDefinition,
    max_concurrent_backfill: int,
    api_base_url: str,
    project_root: str = "",
) -> dg.AssetsDefinition:
    """Factory: create a production NBA API ingestion asset."""

    @dg.asset(
        key=dg.AssetKey(["nba_raw", table_config.name]),
        description=table_config.description,
        partitions_def=daily_partitions,
        backfill_policy=dg.BackfillPolicy.multi_run(
            max_partitions_per_run=max_concurrent_backfill
        ),
        kinds={"api", "duckdb"},
        group_name="nba_raw_ingestion",
        metadata={
            "api_endpoint": table_config.endpoint,
            "api_base_url": api_base_url,
            "partition_type": "daily",
        },
        tags={"source": "nba_api", "layer": "raw"},
    )
    def nba_api_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        import json
        import os

        import requests

        partition_date = context.partition_key
        endpoint = table_config.endpoint
        context.log.info(
            f"Fetching NBA data from {api_base_url}/{endpoint} for {partition_date}"
        )

        response = requests.get(
            f"{api_base_url}/{endpoint}",
            params={"dates[]": partition_date},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json().get("data", [])

        # Write as Parquet for concurrent-safe access
        out_dir = os.path.join(project_root, "data", "raw", endpoint)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{partition_date}.parquet")

        import duckdb

        conn = duckdb.connect()
        records = [
            {"partition_date": partition_date, "data": json.dumps(r)} for r in data
        ]
        if records:
            conn.execute(
                f"COPY (SELECT * FROM '{json.dumps(records)}') TO '{out_path}' (FORMAT PARQUET)"
            )
        else:
            conn.execute(
                f"COPY (SELECT '{partition_date}'::DATE as partition_date, ''::VARCHAR as data WHERE false) TO '{out_path}' (FORMAT PARQUET)"
            )
        conn.close()

        return dg.MaterializeResult(
            metadata={
                "record_count": len(data),
                "partition_date": partition_date,
                "api_endpoint": f"{api_base_url}/{endpoint}",
                "output_path": out_path,
            }
        )

    return nba_api_asset


def _make_demo_asset(
    table_config: NbaApiTableConfig,
    daily_partitions: dg.DailyPartitionsDefinition,
    max_concurrent_backfill: int,
    api_base_url: str,
    project_root: str = "",
) -> dg.AssetsDefinition:
    """Factory: create a demo NBA API ingestion asset with synthetic data."""

    @dg.asset(
        key=dg.AssetKey(["nba_raw", table_config.name]),
        description=table_config.description,
        partitions_def=daily_partitions,
        backfill_policy=dg.BackfillPolicy.multi_run(
            max_partitions_per_run=max_concurrent_backfill
        ),
        kinds={"api", "duckdb"},
        group_name="nba_raw_ingestion",
        metadata={
            "api_endpoint": table_config.endpoint,
            "api_base_url": api_base_url,
            "partition_type": "daily",
            "demo_mode": "true",
        },
        tags={"source": "nba_api", "layer": "raw", "demo_mode": "true"},
    )
    def nba_api_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        import json
        import os

        partition_date = context.partition_key
        name = table_config.name
        context.log.info(
            f"[DEMO] Generating synthetic NBA {name} data for {partition_date}"
        )

        demo_data = _generate_demo_data(
            name, partition_date, table_config.expected_row_count
        )

        # Write as Parquet files - each table gets its own directory,
        # each partition gets its own file. No DuckDB lock contention.
        out_dir = os.path.join(project_root, "data", "raw", name)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{partition_date}.parquet")

        import duckdb

        conn = duckdb.connect()
        records = [
            {"partition_date": partition_date, "data": json.dumps(r)}
            for r in demo_data
        ]
        conn.sql(
            "CREATE TABLE _tmp (partition_date VARCHAR, data VARCHAR)"
        )
        for rec in records:
            conn.execute(
                "INSERT INTO _tmp VALUES (?, ?)",
                [rec["partition_date"], rec["data"]],
            )
        conn.execute(f"COPY _tmp TO '{out_path}' (FORMAT PARQUET)")
        conn.close()

        return dg.MaterializeResult(
            metadata={
                "record_count": len(demo_data),
                "partition_date": partition_date,
                "output_path": out_path,
                "demo_mode": True,
            }
        )

    return nba_api_asset


@dataclass
class NbaApiComponent(Component, Resolvable):
    """Ingests NBA game data from an external API with daily partitions.

    Fetches data from the balldontlie.io NBA API and writes raw JSON
    records to DuckDB tables. Each table is daily-partitioned to support
    efficient backfills across historical NBA seasons.

    Attributes:
        tables: List of API tables to ingest.
        api_base_url: Base URL for the NBA data API.
        season_start: Start date for the NBA season partition window.
        demo_mode: If true, generates realistic fake data instead of calling API.
        max_concurrent_backfill: Max partitions to process in a single backfill run.
    """

    tables: Sequence[NbaApiTableConfig] = field(default_factory=list)
    api_base_url: str = "https://api.balldontlie.io/v1"
    season_start: str = "2024-10-22"
    demo_mode: bool = False
    max_concurrent_backfill: int = 10

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        from pathlib import Path

        # Resolve project root from the component defs path
        # context.path = .../src/dbt_partitions_components/defs/nba_api_ingestion
        project_root = str(Path(context.path).parents[3])

        daily_partitions = dg.DailyPartitionsDefinition(
            start_date=self.season_start,
        )

        factory = _make_demo_asset if self.demo_mode else _make_production_asset
        assets = [
            factory(
                table_config=tc,
                daily_partitions=daily_partitions,
                max_concurrent_backfill=self.max_concurrent_backfill,
                api_base_url=self.api_base_url,
                project_root=project_root,
            )
            for tc in self.tables
        ]

        return dg.Definitions(assets=assets)
