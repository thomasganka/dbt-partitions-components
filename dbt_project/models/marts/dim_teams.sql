with team_stats as (
    select * from {{ ref('stg_team_stats') }}
),

latest_stats as (
    select
        *,
        row_number() over (partition by team_abbreviation order by stat_date desc) as rn
    from team_stats
)

select
    team_abbreviation,
    team_name,
    stat_date as last_updated,
    wins,
    losses,
    wins + losses as games_played,
    case when (wins + losses) > 0
        then round(wins::float / (wins + losses), 3)
        else 0.0
    end as win_pct,
    pts_per_game,
    reb_per_game,
    ast_per_game,
    opp_pts_per_game,
    fg_pct,
    fg3_pct,
    ft_pct,
    net_rating,
    case
        when net_rating > 5 then 'Elite'
        when net_rating > 2 then 'Above Average'
        when net_rating > -2 then 'Average'
        when net_rating > -5 then 'Below Average'
        else 'Rebuilding'
    end as team_tier
from latest_stats
where rn = 1
