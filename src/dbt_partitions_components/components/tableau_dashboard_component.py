"""Tableau Dashboard Refresh Component.

Triggers a Tableau workbook extract refresh after upstream dbt models
have completed, ensuring dashboards reflect the latest NBA analytics.
"""

from dataclasses import dataclass, field
from typing import Any, List, Mapping, Sequence

import dagster as dg
from dagster.components import Component, ComponentLoadContext, Resolvable
from pydantic import BaseModel


class TableauWorkbookConfig(BaseModel):
    """Configuration for a Tableau workbook to refresh."""

    name: str
    workbook_id: str
    description: str = ""
    deps: List[str] = []


def _make_tableau_asset(
    wb: TableauWorkbookConfig,
    server_url: str,
    site_id: str,
    token_name: str,
    demo_mode: bool,
) -> dg.AssetsDefinition:
    """Factory function to create a Tableau dashboard refresh asset."""
    dep_keys = [dg.AssetKey(d.split("/")) for d in wb.deps]

    if demo_mode:

        @dg.asset(
            key=dg.AssetKey(["tableau", wb.name]),
            description=wb.description or f"Tableau dashboard: {wb.name}",
            deps=dep_keys,
            kinds={"tableau"},
            group_name="tableau_dashboards",
            metadata={
                "server_url": server_url,
                "site_id": site_id,
                "workbook_id": wb.workbook_id,
                "demo_mode": "true",
            },
            tags={
                "layer": "presentation",
                "tool": "tableau",
                "demo_mode": "true",
            },
        )
        def tableau_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import random
            import time

            context.log.info(
                f"[DEMO] Simulating Tableau extract refresh for: {wb.name}"
            )
            time.sleep(random.uniform(0.5, 1.5))

            return dg.MaterializeResult(
                metadata={
                    "refresh_job_id": f"demo-job-{random.randint(1000, 9999)}",
                    "workbook": wb.name,
                    "demo_mode": True,
                    "refresh_duration_seconds": round(random.uniform(15.0, 45.0), 1),
                    "rows_refreshed": random.randint(5000, 50000),
                }
            )

    else:

        @dg.asset(
            key=dg.AssetKey(["tableau", wb.name]),
            description=wb.description or f"Tableau dashboard: {wb.name}",
            deps=dep_keys,
            kinds={"tableau"},
            group_name="tableau_dashboards",
            metadata={
                "server_url": server_url,
                "site_id": site_id,
                "workbook_id": wb.workbook_id,
            },
            tags={"layer": "presentation", "tool": "tableau"},
        )
        def tableau_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import requests

            context.log.info(
                f"Triggering Tableau extract refresh for workbook: {wb.name}"
            )

            auth_url = f"{server_url}/api/3.21/auth/signin"
            auth_payload = {
                "credentials": {
                    "personalAccessTokenName": token_name,
                    "personalAccessTokenSecret": "{{ env.TABLEAU_TOKEN_SECRET }}",
                    "site": {"contentUrl": site_id},
                }
            }
            auth_resp = requests.post(auth_url, json=auth_payload, timeout=30)
            auth_resp.raise_for_status()
            token = auth_resp.json()["credentials"]["token"]

            refresh_url = f"{server_url}/api/3.21/sites/{site_id}/workbooks/{wb.workbook_id}/refresh"
            headers = {"X-Tableau-Auth": token}
            refresh_resp = requests.post(
                refresh_url, headers=headers, json={}, timeout=30
            )
            refresh_resp.raise_for_status()
            job_id = refresh_resp.json().get("job", {}).get("id", "unknown")

            return dg.MaterializeResult(
                metadata={
                    "refresh_job_id": job_id,
                    "workbook": wb.name,
                    "server": server_url,
                }
            )

    return tableau_asset


@dataclass
class TableauDashboardComponent(Component, Resolvable):
    """Refreshes Tableau dashboard extracts after upstream dbt models complete.

    Connects to the Tableau REST API to trigger extract refreshes for
    configured workbooks. Each workbook asset depends on its upstream
    dbt report models so lineage flows naturally through the pipeline.

    Attributes:
        workbooks: Tableau workbooks to refresh.
        server_url: Tableau Server or Tableau Cloud URL.
        site_id: Tableau site identifier.
        token_name: Personal access token name for authentication.
        demo_mode: If true, simulates refresh without calling Tableau API.
    """

    workbooks: Sequence[TableauWorkbookConfig] = field(default_factory=list)
    server_url: str = "https://tableau.example.com"
    site_id: str = "nba-analytics"
    token_name: str = "dagster-service-account"
    demo_mode: bool = False

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        assets = [
            _make_tableau_asset(
                wb=wb,
                server_url=self.server_url,
                site_id=self.site_id,
                token_name=self.token_name,
                demo_mode=self.demo_mode,
            )
            for wb in self.workbooks
        ]
        return dg.Definitions(assets=assets)
