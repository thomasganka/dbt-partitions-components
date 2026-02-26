with raw_games as (
    select
        partition_date,
        data::JSON as game_data
    from {{ source('nba_raw', 'games') }}
),

parsed as (
    select
        game_data->>'id' as game_id,
        cast(partition_date as date) as game_date,
        cast(game_data->>'season' as int) as season,
        game_data->>'status' as game_status,
        game_data->'home_team'->>'abbreviation' as home_team,
        game_data->'home_team'->>'full_name' as home_team_name,
        game_data->'visitor_team'->>'abbreviation' as away_team,
        game_data->'visitor_team'->>'full_name' as away_team_name,
        cast(game_data->>'home_team_score' as int) as home_score,
        cast(game_data->>'visitor_team_score' as int) as away_score
    from raw_games
)

select
    game_id,
    game_date,
    season,
    game_status,
    home_team,
    home_team_name,
    away_team,
    away_team_name,
    home_score,
    away_score,
    home_score - away_score as score_differential,
    case when home_score > away_score then home_team else away_team end as winner,
    case when home_score > away_score then away_team else home_team end as loser
from parsed
