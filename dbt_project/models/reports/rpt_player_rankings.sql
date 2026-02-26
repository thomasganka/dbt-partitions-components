with player_perf as (
    select * from {{ ref('fct_player_performance') }}
),

player_aggregates as (
    select
        player_id,
        team,
        count(*) as games_played,
        round(avg(points), 1) as avg_points,
        round(avg(rebounds), 1) as avg_rebounds,
        round(avg(assists), 1) as avg_assists,
        round(avg(steals), 1) as avg_steals,
        round(avg(blocks), 1) as avg_blocks,
        round(avg(turnovers), 1) as avg_turnovers,
        round(avg(efficiency_rating), 1) as avg_efficiency,
        round(avg(fantasy_points), 1) as avg_fantasy_points,
        round(avg(fg_pct), 3) as avg_fg_pct,
        round(avg(fg3_pct), 3) as avg_fg3_pct,
        round(avg(ft_pct), 3) as avg_ft_pct,
        sum(case when is_double_double then 1 else 0 end) as double_doubles,
        sum(case when is_triple_double then 1 else 0 end) as triple_doubles,
        max(points) as max_points,
        max(rebounds) as max_rebounds,
        max(assists) as max_assists
    from player_perf
    group by player_id, team
)

select
    player_id,
    team,
    games_played,
    avg_points,
    avg_rebounds,
    avg_assists,
    avg_steals,
    avg_blocks,
    avg_turnovers,
    avg_efficiency,
    avg_fantasy_points,
    avg_fg_pct,
    avg_fg3_pct,
    avg_ft_pct,
    double_doubles,
    triple_doubles,
    max_points,
    max_rebounds,
    max_assists,
    row_number() over (order by avg_efficiency desc) as efficiency_rank,
    row_number() over (order by avg_points desc) as scoring_rank,
    row_number() over (order by avg_fantasy_points desc) as fantasy_rank
from player_aggregates
order by avg_efficiency desc
