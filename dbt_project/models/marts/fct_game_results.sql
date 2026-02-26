with games as (
    select * from {{ ref('stg_games') }}
),

team_info as (
    select * from {{ ref('stg_team_stats') }}
)

select
    g.game_id,
    g.game_date,
    g.season,
    g.game_status,
    g.home_team,
    g.home_team_name,
    g.away_team,
    g.away_team_name,
    g.home_score,
    g.away_score,
    g.score_differential,
    abs(g.score_differential) as margin_of_victory,
    g.winner,
    g.loser,
    case
        when abs(g.score_differential) <= 3 then 'Nail Biter'
        when abs(g.score_differential) <= 10 then 'Close Game'
        when abs(g.score_differential) <= 20 then 'Comfortable Win'
        else 'Blowout'
    end as game_type,
    g.home_score + g.away_score as total_points,
    case
        when g.home_score > g.away_score then 'Home'
        else 'Away'
    end as winner_location
from games g
