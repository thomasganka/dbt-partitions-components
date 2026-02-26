with teams as (
    select * from {{ ref('dim_teams') }}
),

game_results as (
    select * from {{ ref('fct_game_results') }}
),

home_away_splits as (
    select
        home_team as team,
        count(*) as home_games,
        sum(case when winner = home_team then 1 else 0 end) as home_wins
    from game_results
    group by home_team
),

recent_form as (
    select
        winner as team,
        count(*) as recent_wins
    from game_results
    group by winner
)

select
    t.team_abbreviation,
    t.team_name,
    t.wins,
    t.losses,
    t.games_played,
    t.win_pct,
    t.pts_per_game,
    t.opp_pts_per_game,
    t.net_rating,
    t.team_tier,
    t.fg_pct,
    t.fg3_pct,
    t.ft_pct,
    coalesce(h.home_games, 0) as home_games_played,
    coalesce(h.home_wins, 0) as home_wins,
    coalesce(rf.recent_wins, 0) as total_wins_in_data,
    row_number() over (order by t.win_pct desc) as overall_rank
from teams t
left join home_away_splits h on t.team_abbreviation = h.team
left join recent_form rf on t.team_abbreviation = rf.team
order by t.win_pct desc
