from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json

import duckdb
import pandas as pd

DUCKDB_PATH = "/opt/airflow/duckdb/warehouse.duckdb"
OUTPUT_DIR = Path("/opt/airflow/data/analytics")
MARKDOWN_PATH = OUTPUT_DIR / "analytics_summary.md"
JSON_PATH = OUTPUT_DIR / "analytics_summary.json"


def fetch__df(con: duckdb.DuckDBPyConnection, query: str):
    return con.execute(query).fetchdf()


def run():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DUCKDB_PATH)

    overall_df = fetch__df(
        con,
        """
        SELECT
            count(*) AS players,
            sum(CASE WHEN is_payer THEN 1 ELSE 0 END) AS payers,
            round(
                100.0 * sum(CASE WHEN is_payer THEN 1 ELSE 0 END) / count(*),
                2
            ) AS payer_rate_pct
        FROM main_mart.dim_players
        """,)

    channel_df = fetch__df(
        con,
        """
        SELECT
            acquisition_channel,
            count(*) AS players,
            sum(CASE WHEN is_payer THEN 1 ELSE 0 END) AS payers,
            round(
                100.0 * sum(CASE WHEN is_payer THEN 1 ELSE 0 END) / count(*),
                2
            ) AS payer_rate_pct,
            round(sum(total_gross_usd), 2) AS revenue_usd,
            round(
                sum(total_gross_usd)
                / nullif(sum(CASE WHEN is_payer THEN 1 ELSE 0 END), 0),
                2
            ) AS revenue_per_payer_usd
        FROM main_mart.dim_players
        GROUP BY acquisition_channel
        ORDER BY payer_rate_pct DESC, players DESC
        """,)

    country_df = fetch__df(
        con,
        """
        SELECT
            country_name,
            round(sum(total_gross_usd), 2) AS revenue_usd,
            round(
                100.0 * sum(total_gross_usd)
                / (SELECT sum(total_gross_usd) FROM main_mart.dim_players),
                2
            ) AS revenue_share_pct
        FROM main_mart.dim_players
        GROUP BY country_name
        ORDER BY revenue_usd DESC
        LIMIT 5
        """,)

    top_player_df = fetch__df(
        con,
        """
        SELECT
            country_name,
            player_id,
            round(total_gross_usd, 2) AS revenue_usd,
            purchase_count,
            pct_of_country_revenue
        FROM main_mart.fct_top_players_by_country
        WHERE country_rank = 1
        ORDER BY revenue_usd DESC
        LIMIT 5
        """,)

    top_day_df = fetch__df(
        con,
        """
        SELECT
            date_day,
            round(gross_revenue_usd, 2) AS gross_revenue_usd,
            orders,
            unique_payers
        FROM main_mart.fct_daily_revenue
        ORDER BY gross_revenue_usd DESC, date_day DESC
        LIMIT 1
        """,)

    rolling_df = fetch__df(
        con,
        """
        SELECT
            date_day,
            round(rolling_7d_revenue_usd, 2) AS rolling_7d_revenue_usd,
            rolling_7d_orders
        FROM main_mart.fct_revenue_rolling_7d
        ORDER BY rolling_7d_revenue_usd DESC, date_day DESC
        LIMIT 1
        """,)

    overall = overall_df.iloc[0].to_dict()
    best_channel = channel_df.iloc[0].to_dict()
    top_country = country_df.iloc[0].to_dict()
    top_country_player = top_player_df[top_player_df["country_name"] == top_country["country_name"]].iloc[0].to_dict()
    top_day = top_day_df.iloc[0].to_dict()
    top_rolling = rolling_df.iloc[0].to_dict()

    insights = [
        {
            "title": "Which acquisition channel converts best?\n\n",
            "summary": (
                f"{best_channel['acquisition_channel']} has the strongest payer conversion: "
                f"{int(best_channel['payers'])} payers out of {int(best_channel['players'])} players "
                f"({best_channel['payer_rate_pct']:.2f}%), versus an overall payer rate of "
                f"{overall['payer_rate_pct']:.2f}%."
            ),
        },
        {
            "title": "Which market contributes the most revenue?\n\n",
            "summary": (
                f"{top_country['country_name']} is the top market with ${top_country['revenue_usd']:.2f} "
                f"or {top_country['revenue_share_pct']:.2f}% of total revenue. "
                f"Its top spender is player {int(top_country_player['player_id'])}, who generated "
                f"${top_country_player['revenue_usd']:.2f} across {int(top_country_player['purchase_count'])} "
                f"purchases and accounts for {top_country_player['pct_of_country_revenue']:.2f}% "
                f"of that country's revenue."
            ),
        },
        {
            "title": "How concentrated is recent revenue?\n\n",
            "summary": (
                f"The highest single-day revenue was ${top_day['gross_revenue_usd']:.2f} on "
                f"{top_day['date_day']}, produced by {int(top_day['orders'])} orders from "
                f"{int(top_day['unique_payers'])} payers. The strongest 7-day window reached "
                f"${top_rolling['rolling_7d_revenue_usd']:.2f} by {top_rolling['date_day']} across "
                f"{int(top_rolling['rolling_7d_orders'])} rolling orders."
            ),
        },
    ]

    generated_at = datetime.now(timezone.utc).isoformat()
    markdown_lines = [
        "# Summary",
        "",
        f"Generated at: {generated_at}",
        "",
        "## Insights:",
        "",
    ]

    for idx, insight in enumerate(insights, start=1):
        markdown_lines.append(f"{idx}. **{insight['title']}** {insight['summary']}")
        markdown_lines.append("")

    MARKDOWN_PATH.write_text("\n".join(markdown_lines), encoding="utf-8")

    payload = {
        "generated_at": generated_at,
        "overall": overall,
        "top_channel": best_channel,
        "top_country": top_country,
        "top_country_player": top_country_player,
        "top_day": top_day,
        "top_rolling_window": top_rolling,
        "insights": insights,
    }
    JSON_PATH.write_text(json.dumps(payload, indent= 2, default =str), encoding="utf-8")

    print(f"Wrote analytics summary to {MARKDOWN_PATH}")
    print(f"Wrote analytics payload to {JSON_PATH}")
