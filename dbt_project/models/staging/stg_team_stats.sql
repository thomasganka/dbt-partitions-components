with raw_stats as (
    select
        partition_date,
        data::JSON as stat_data
    from {{ source('nba_raw', 'team_stats') }}
)

select
    stat_data->'team'->>'abbreviation' as team_abbreviation,
    stat_data->'team'->>'full_name' as team_name,
    cast(partition_date as date) as stat_date,
    cast(stat_data->>'wins' as int) as wins,
    cast(stat_data->>'losses' as int) as losses,
    cast(stat_data->>'pts_per_game' as float) as pts_per_game,
    cast(stat_data->>'reb_per_game' as float) as reb_per_game,
    cast(stat_data->>'ast_per_game' as float) as ast_per_game,
    cast(stat_data->>'opp_pts_per_game' as float) as opp_pts_per_game,
    cast(stat_data->>'fg_pct' as float) as fg_pct,
    cast(stat_data->>'fg3_pct' as float) as fg3_pct,
    cast(stat_data->>'ft_pct' as float) as ft_pct,
    round(cast(stat_data->>'pts_per_game' as float) - cast(stat_data->>'opp_pts_per_game' as float), 1) as net_rating
from raw_stats
