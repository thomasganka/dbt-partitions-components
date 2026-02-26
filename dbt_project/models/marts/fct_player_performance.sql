with player_stats as (
    select * from {{ ref('stg_player_stats') }}
)

select
    stat_id,
    game_id,
    player_id,
    team,
    game_date,
    minutes_played,
    points,
    rebounds,
    assists,
    steals,
    blocks,
    turnovers,
    field_goals_made,
    field_goals_attempted,
    fg_pct,
    three_pointers_made,
    three_pointers_attempted,
    fg3_pct,
    free_throws_made,
    free_throws_attempted,
    ft_pct,
    personal_fouls,

    -- Efficiency rating: PTS + REB + AST + STL + BLK - (FGA - FGM) - (FTA - FTM) - TO
    (points + rebounds + assists + steals + blocks
     - (field_goals_attempted - field_goals_made)
     - (free_throws_attempted - free_throws_made)
     - turnovers) as efficiency_rating,

    -- Fantasy points (standard scoring)
    (points * 1.0
     + rebounds * 1.2
     + assists * 1.5
     + steals * 3.0
     + blocks * 3.0
     - turnovers * 1.0) as fantasy_points,

    -- Double-double detection
    case when (
        (case when points >= 10 then 1 else 0 end)
        + (case when rebounds >= 10 then 1 else 0 end)
        + (case when assists >= 10 then 1 else 0 end)
        + (case when steals >= 10 then 1 else 0 end)
        + (case when blocks >= 10 then 1 else 0 end)
    ) >= 2 then true else false end as is_double_double,

    case when (
        (case when points >= 10 then 1 else 0 end)
        + (case when rebounds >= 10 then 1 else 0 end)
        + (case when assists >= 10 then 1 else 0 end)
        + (case when steals >= 10 then 1 else 0 end)
        + (case when blocks >= 10 then 1 else 0 end)
    ) >= 3 then true else false end as is_triple_double

from player_stats
