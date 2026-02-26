with raw_stats as (
    select
        partition_date,
        data::JSON as stat_data
    from {{ source('nba_raw', 'player_stats') }}
),

parsed as (
    select
        stat_data->>'id' as stat_id,
        stat_data->>'game_id' as game_id,
        stat_data->>'player_id' as player_id,
        stat_data->'team'->>'abbreviation' as team,
        cast(partition_date as date) as game_date,
        stat_data->>'min' as minutes_played,
        cast(stat_data->>'pts' as int) as points,
        cast(stat_data->>'reb' as int) as rebounds,
        cast(stat_data->>'ast' as int) as assists,
        cast(stat_data->>'stl' as int) as steals,
        cast(stat_data->>'blk' as int) as blocks,
        cast(stat_data->>'turnover' as int) as turnovers,
        cast(stat_data->>'fgm' as int) as field_goals_made,
        cast(stat_data->>'fga' as int) as field_goals_attempted,
        cast(stat_data->>'fg3m' as int) as three_pointers_made,
        cast(stat_data->>'fg3a' as int) as three_pointers_attempted,
        cast(stat_data->>'ftm' as int) as free_throws_made,
        cast(stat_data->>'fta' as int) as free_throws_attempted,
        cast(stat_data->>'pf' as int) as personal_fouls
    from raw_stats
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
    case when field_goals_attempted > 0
        then round(field_goals_made::float / field_goals_attempted, 3)
        else 0.0
    end as fg_pct,
    three_pointers_made,
    three_pointers_attempted,
    case when three_pointers_attempted > 0
        then round(three_pointers_made::float / three_pointers_attempted, 3)
        else 0.0
    end as fg3_pct,
    free_throws_made,
    free_throws_attempted,
    case when free_throws_attempted > 0
        then round(free_throws_made::float / free_throws_attempted, 3)
        else 0.0
    end as ft_pct,
    personal_fouls
from parsed
