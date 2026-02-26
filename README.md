# NBA Analytics Pipeline: Components, Partitions & dbt

A Dagster demo project showcasing how to build a production-grade NBA analytics pipeline using **Components**, **Partitions**, **dbt**, **Sensors**, and **Declarative Automation**. Data flows from the NBA API through dbt transformations and into Tableau dashboards, with daily partitions enabling efficient backfills and concurrency control. The project also demonstrates event-driven pipelines via Excel file sensors and self-scheduling assets using `AutomationCondition.on_cron`.

## Pipeline Overview

```
NBA API (balldontlie.io)              Excel Files (Front Office)
    │                                     │
    ├── nba_raw/games          ─┐         ├── excel/salary_cap        ─┐
    ├── nba_raw/player_stats    │ Daily   └── excel/trade_targets      │ Sensor-
    └── nba_raw/team_stats     ─┘ Partitioned                         ─┘ Triggered
         │
    dbt Models (DuckDB)
         │
    ┌────┴────────────────────┐
    │  Staging Layer          │
    │  ├── stg_games          │
    │  ├── stg_player_stats   │
    │  └── stg_team_stats     │
    │                         │
    │  Marts Layer            │
    │  ├── fct_game_results   │
    │  ├── fct_player_perf    │
    │  └── dim_teams          │
    │                         │
    │  Reports Layer          │
    │  ├── rpt_team_standings │
    │  └── rpt_player_rankings│
    └─────────────────────────┘
         │                    │
    Tableau Dashboards   Automated Analytics (on_cron)
    ├── nba_team_standings    ├── league_daily_summary
    └── nba_player_analytics  └── player_watchlist_alerts
```

## Quick Start

```bash
# Install dependencies
uv sync

# Parse dbt project (generates manifest)
uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project

# Validate all definitions
uv run dg check defs

# Launch the Dagster UI
uv run dg dev
```

Open http://localhost:3000 to see the asset graph, trigger materializations, and run backfills.

## Project Structure

```
dbt-partitions-components/
├── src/dbt_partitions_components/
│   ├── definitions.py                           # Dagster entry point
│   ├── components/                              # Custom component definitions
│   │   ├── nba_api_component.py                 # NBA API ingestion + partitions
│   │   ├── partitioned_dbt_component.py         # dbt with daily partitions
│   │   ├── tableau_dashboard_component.py       # Tableau extract refresh
│   │   ├── scheduled_job_component.py           # Flexible job scheduling
│   │   ├── excel_sensor_component.py            # Excel file sensor + ingestion
│   │   └── automation_condition_component.py    # Declarative on_cron automation
│   └── defs/                                    # Component instances (YAML)
│       ├── nba_api_ingestion/defs.yaml
│       ├── nba_dbt_models/defs.yaml
│       ├── tableau_dashboard/defs.yaml
│       ├── schedules/defs.yaml
│       ├── excel_sensor/defs.yaml               # Excel file watch config
│       └── automation_conditions/defs.yaml      # on_cron asset config
├── dbt_project/                                 # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml                             # DuckDB profile for local dev
│   └── models/
│       ├── staging/                             # Raw data cleaning
│       ├── marts/                               # Business logic
│       └── reports/                             # Tableau-ready reports
├── data/
│   ├── raw/                                     # Parquet files from API
│   ├── excel/                                   # Excel files watched by sensors
│   └── analytics/                               # Automation condition outputs
└── pyproject.toml
```

---

## How Components Work

Dagster Components are the building blocks of this pipeline. Each component is a Python class that produces Dagster definitions (assets, resources, schedules) from YAML configuration.

### Anatomy of a Component

```python
from dataclasses import dataclass, field
from dagster.components import Component, ComponentLoadContext, Resolvable

@dataclass
class MyComponent(Component, Resolvable):
    """A reusable component that generates assets from config."""

    # These fields become YAML attributes
    tables: Sequence[TableConfig] = field(default_factory=list)
    demo_mode: bool = False

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        # Return assets, resources, schedules, etc.
        if self.demo_mode:
            return self._build_demo_defs(context)
        return self._build_production_defs(context)
```

### Configuration via YAML

Each component instance is configured in a `defs.yaml` file:

```yaml
type: my_package.components.my_component.MyComponent

attributes:
  demo_mode: true
  tables:
    - name: users
      endpoint: /api/users
```

### Key Concepts

1. **`Component` base class** - Requires implementing `build_defs()` which returns `dg.Definitions`
2. **`Resolvable` mixin** - Enables YAML-to-Python deserialization automatically
3. **Factory functions** - Use factory functions (not closure default args) to create multiple assets in a loop, because `@dg.asset` introspects function parameters
4. **`demo_mode` pattern** - Every component should have a `demo_mode: bool` flag that enables local execution without external dependencies

### Components in This Project

| Component | File | Purpose |
|-----------|------|---------|
| `NbaApiComponent` | `nba_api_component.py` | Ingests NBA data from API with daily partitions |
| `PartitionedDbtComponent` | `partitioned_dbt_component.py` | Runs dbt models with partition-aware variable injection |
| `TableauDashboardComponent` | `tableau_dashboard_component.py` | Refreshes Tableau workbook extracts |
| `ScheduledJobComponent` | `scheduled_job_component.py` | Creates scheduled jobs using asset selection syntax |
| `ExcelSensorComponent` | `excel_sensor_component.py` | Watches Excel files and triggers asset materialization on change |
| `AutomationConditionComponent` | `automation_condition_component.py` | Self-scheduling assets using `AutomationCondition.on_cron` |

---

## How Partitions Work

Partitions are central to this project. They allow you to divide data processing into discrete time slices, enabling:

- **Incremental backfills** - Process only the date ranges you need
- **Concurrency control** - Limit how many partitions run simultaneously
- **Idempotent runs** - Re-run any partition without side effects

### Defining Partitions

```python
daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2024-10-22",  # NBA season start
)
```

### Attaching Partitions to Assets

```python
@dg.asset(
    partitions_def=daily_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(
        max_partitions_per_run=10  # Process up to 10 dates per run
    ),
)
def my_asset(context: dg.AssetExecutionContext):
    partition_date = context.partition_key  # e.g., "2024-11-15"
    # Process data for this specific date
```

### Backfill Policy

The `BackfillPolicy` controls how partitions are processed during a backfill:

```python
# Process multiple partitions in separate runs (good for API rate limits)
backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=10)

# Process all selected partitions in a single run (good for bulk operations)
backfill_policy=dg.BackfillPolicy.single_run()
```

### Running a Backfill

In the Dagster UI:
1. Navigate to an asset
2. Click "Materialize" > "Backfill"
3. Select date range (e.g., entire 2024-25 NBA season)
4. Dagster automatically chunks the range based on your `BackfillPolicy`

### Partition-Aware dbt

The `PartitionedDbtComponent` passes the partition date to dbt as a variable:

```python
dbt_vars = {"execution_date": partition_date}
yield from dbt.cli(
    ["build", "--vars", str(dbt_vars)],
    context=context,
).stream()
```

In dbt models, you can then use:
```sql
WHERE game_date = '{{ var("execution_date") }}'
```

---

## How dbt Integration Works

### dbt Project Setup

The dbt project lives in `dbt_project/` and uses DuckDB as the local warehouse:

```yaml
# profiles.yml
nba_analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: nba_analytics.duckdb
```

### Model Layers

| Layer | Directory | Purpose |
|-------|-----------|---------|
| **Staging** | `models/staging/` | Clean and type raw API data |
| **Marts** | `models/marts/` | Business logic, fact/dimension tables |
| **Reports** | `models/reports/` | Tableau-ready aggregations |

### Connecting API Sources to dbt

dbt sources in `staging/schema.yml` must match the upstream asset keys:

```yaml
sources:
  - name: nba_raw          # Matches AssetKey(["nba_raw", ...])
    tables:
      - name: games         # Matches AssetKey(["nba_raw", "games"])
      - name: player_stats  # Matches AssetKey(["nba_raw", "player_stats"])
      - name: team_stats    # Matches AssetKey(["nba_raw", "team_stats"])
```

This automatically creates the dependency: `nba_raw/games -> stg_games`

### dbt as a Component

Instead of using the built-in `DbtProjectComponent`, this project uses a custom `PartitionedDbtComponent` that adds:

- Daily partitions to all dbt models
- Partition-aware variable injection (`execution_date`)
- Backfill policy for concurrency control
- Demo mode support with local DuckDB profiles

```yaml
# defs.yaml
type: dbt_partitions_components.components.partitioned_dbt_component.PartitionedDbtComponent

attributes:
  dbt_project_dir: dbt_project
  partition_start_date: "2024-10-22"
  max_partitions_per_run: 10
  demo_mode: true
```

---

## Schedules

Four schedules orchestrate the daily pipeline:

| Schedule | Cron | Selection | Purpose |
|----------|------|-----------|---------|
| `nba_daily_ingestion` | `0 6 * * *` | `group:nba_raw_ingestion` | Fetch latest NBA data |
| `nba_dbt_daily_build` | `30 6 * * *` | `kind:dbt` | Transform data through dbt |
| `nba_tableau_refresh` | `0 7 * * *` | `group:tableau_dashboards` | Refresh Tableau extracts |
| `nba_weekly_full_refresh` | `0 2 * * 0` | `*` | Full pipeline rebuild (Sundays) |

Schedules use **asset selection syntax** instead of hardcoded asset keys, making them resilient to changes in the asset graph.

---

## How Sensors Work

Sensors enable **event-driven pipelines** -- instead of running on a fixed schedule, assets are materialized in response to external events. This project uses a file sensor that watches Excel files for changes.

### The Use Case

The NBA front office maintains salary cap data and trade target watchlists in Excel spreadsheets. When an analyst updates these files (e.g., after a trade), Dagster automatically detects the change and reloads the data into the pipeline.

### How the Excel Sensor Works

```
Front Office updates nba_salary_cap.xlsx
    │
    ▼
salary_cap_file_sensor (checks every 30s)
    │  Compares file modification time to cursor
    │  If changed → emits RunRequest
    ▼
salary_cap_refresh job
    │
    ▼
excel/salary_cap asset materializes
    │  Reads Excel → writes Parquet
    ▼
Data available for downstream analytics
```

The sensor tracks file changes using the file's **modification time** (`mtime`). Each evaluation:

1. Checks if the Excel file exists (in demo mode, generates a sample file if missing)
2. Reads the file's `mtime` and compares it to the stored cursor
3. If `mtime` has changed, emits a `RunRequest` that triggers the associated job
4. Updates the cursor so the same change isn't processed twice

### Sensor Configuration

```yaml
# defs/excel_sensor/defs.yaml
type: ...excel_sensor_component.ExcelSensorComponent

attributes:
  demo_mode: true
  files:
    - name: salary_cap
      file_path: "data/excel/nba_salary_cap.xlsx"
      sheet_name: "Sheet1"
      description: "NBA team salary cap data"
      group_name: excel_ingestion

    - name: trade_targets
      file_path: "data/excel/nba_trade_targets.xlsx"
      sheet_name: "Sheet1"
      description: "Scouting trade target watchlist"
      group_name: excel_ingestion
```

Each file entry creates:
- An **asset** (`excel/salary_cap`) that reads the Excel file and writes Parquet
- A **job** (`salary_cap_refresh`) that targets the asset
- A **sensor** (`salary_cap_file_sensor`) that watches the file and triggers the job

### Testing the Sensor Locally

```bash
# Launch the Dagster UI
uv run dg dev

# In another terminal, modify an Excel file to trigger the sensor:
touch data/excel/nba_salary_cap.xlsx
```

The sensor polls every 30 seconds. After detecting the change, it triggers the `salary_cap_refresh` job in the Dagster UI.

### Key Concepts

- **`RunRequest`** - A signal from the sensor that a job should run. Includes a `run_key` for deduplication.
- **Cursor** - Persistent state stored between sensor evaluations. Used to track the last-seen `mtime` so the same file change isn't processed twice.
- **`minimum_interval_seconds`** - How often the sensor checks for changes (30 seconds in this project).
- **`DefaultSensorStatus.RUNNING`** - The sensor starts automatically when the Dagster daemon launches.

---

## How Declarative Automation Works (on_cron)

Declarative automation is an alternative to traditional schedules. Instead of defining a schedule that triggers a job, you attach an **automation condition** directly to an asset. The asset declares *when it should be materialized*, and the Dagster daemon handles the rest.

### Imperative vs. Declarative Scheduling

| Approach | How it works | Best for |
|----------|-------------|----------|
| **Schedules** (imperative) | "Run this job at 6:00 AM" | Batch pipelines with fixed timing |
| **AutomationCondition.on_cron** (declarative) | "This asset should be fresh as of 8:00 AM" | Assets that self-manage their freshness |

The key difference: declarative automation is **dependency-aware**. An asset with `on_cron("0 8 * * *")` won't just materialize at 8 AM -- it waits until all upstream dependencies have been updated since the last cron tick. This prevents stale data from propagating.

### How on_cron Works

```python
@dg.asset(
    automation_condition=dg.AutomationCondition.on_cron("0 8 * * *"),
    deps=[AssetKey("rpt_team_standings"), AssetKey("rpt_player_rankings")],
)
def league_daily_summary(context):
    # This asset auto-materializes at 8am ET
    # BUT only after rpt_team_standings and rpt_player_rankings
    # have been updated since the last 8am tick
    ...
```

The Dagster daemon evaluates the condition every ~30 seconds:
1. Has the cron tick fired since the last materialization? (e.g., is it past 8:00 AM?)
2. Have all upstream assets been materialized since that cron tick?
3. If both are true --> trigger materialization

### Automation Condition Configuration

```yaml
# defs/automation_conditions/defs.yaml
type: ...automation_condition_component.AutomationConditionComponent

attributes:
  demo_mode: true
  assets:
    - name: league_daily_summary
      cron_schedule: "0 8 * * *"           # Every day at 8am
      deps:
        - "rpt_team_standings"
        - "rpt_player_rankings"
      timezone: US/Eastern

    - name: player_watchlist_alerts
      cron_schedule: "0 12 * * 1-5"        # Weekdays at noon
      deps:
        - "rpt_player_rankings"
      timezone: US/Eastern
```

### Assets in This Project

| Asset | Cron | Dependencies | Purpose |
|-------|------|-------------|---------|
| `analytics/league_daily_summary` | `0 8 * * *` | `rpt_team_standings`, `rpt_player_rankings` | Daily league-wide stats summary |
| `analytics/player_watchlist_alerts` | `0 12 * * 1-5` | `rpt_player_rankings` | Weekday player performance alerts |

### Viewing Automation Status in the UI

In the Dagster UI:
1. Navigate to **Automation** in the left sidebar
2. You'll see the `default_automation_condition_sensor` running automatically
3. Click on any automated asset to see its condition evaluation timeline
4. The asset detail page shows the condition status: waiting for cron tick, waiting for dependencies, or ready to materialize

### When to Use on_cron vs. Schedules

Use **`AutomationCondition.on_cron`** when:
- Assets should only materialize after their dependencies are fresh
- You want assets to self-manage their scheduling
- You're building a declarative, dependency-aware pipeline

Use **traditional schedules** when:
- You need to trigger jobs at exact times regardless of dependency status
- You want explicit control over job execution
- You need to target assets by tags/groups dynamically

---

## Demo Mode

All components support `demo_mode: true` in their YAML configuration. When enabled:

- **NBA API Component**: Generates realistic synthetic NBA data (game scores, player stats, team records) using seeded random generators for deterministic results
- **dbt Component**: Uses local DuckDB profile instead of production warehouse
- **Tableau Component**: Simulates API calls and returns mock refresh metadata
- **Excel Sensor Component**: Auto-generates sample salary cap and trade target Excel files if they don't exist. The sensor watches these generated files.
- **Automation Condition Component**: Generates synthetic league summaries and player watchlist alerts with realistic NBA data

To switch to production, set `demo_mode: false` in each component's `defs.yaml` and configure the real connection credentials.

---

## Validation Commands

```bash
# Validate all component YAML and definitions load correctly
uv run dg check defs

# List all assets with their dependencies
uv run dg list defs

# Parse dbt project (needed after model changes)
uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project

# Check dependency chain in detail
uv run dg list defs --json | python -c "
import sys, json
data = json.load(sys.stdin)
for a in data['assets']:
    deps = a.get('deps', [])
    print(f\"{a['key']} <- {', '.join(deps) if deps else '(source)'}\")
"
```

---

## Extending This Project

### Adding a New API Table

Add a table entry to `nba_api_ingestion/defs.yaml`:

```yaml
tables:
  - name: playoffs
    endpoint: games
    description: "NBA playoff game data"
    expected_row_count: 4
```

### Adding a New dbt Model

1. Create the SQL file in the appropriate `dbt_project/models/` subdirectory
2. Add it to the corresponding `schema.yml`
3. Run `uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project`
4. Run `uv run dg check defs` to verify

### Adding a New Tableau Dashboard

Add a workbook entry to `tableau_dashboard/defs.yaml`:

```yaml
workbooks:
  - name: nba_game_predictions
    workbook_id: "wb-predictions-003"
    description: "ML-powered game outcome predictions"
    deps:
      - "rpt_team_standings"
      - "rpt_player_rankings"
```

### Adding a New Excel File Sensor

Add a file entry to `excel_sensor/defs.yaml`:

```yaml
files:
  - name: injury_reports
    file_path: "data/excel/nba_injury_reports.xlsx"
    sheet_name: "Sheet1"
    description: "Daily injury reports from medical staff"
    group_name: excel_ingestion
```

This creates:
- An `excel/injury_reports` asset
- An `injury_reports_refresh` job
- An `injury_reports_file_sensor` sensor

### Adding a New Automated Asset

Add an asset entry to `automation_conditions/defs.yaml`:

```yaml
assets:
  - name: weekly_power_rankings
    cron_schedule: "0 9 * * 1"       # Mondays at 9am
    description: "Weekly power rankings computed from team performance"
    deps:
      - "rpt_team_standings"
    group_name: automated_analytics
    timezone: US/Eastern
```

This creates an `analytics/weekly_power_rankings` asset that auto-materializes every Monday at 9am ET, after `rpt_team_standings` has been updated.

---

## Learn More

- [Dagster Components Guide](https://docs.dagster.io/guides/build/projects/components)
- [Dagster Partitions](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions)
- [Dagster Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors)
- [Dagster Declarative Automation](https://docs.dagster.io/concepts/automation/declarative-automation)
- [Dagster + dbt Integration](https://docs.dagster.io/integrations/dbt)
- [Dagster University](https://courses.dagster.io/)
