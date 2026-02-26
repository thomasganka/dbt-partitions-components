{% macro create_raw_views() %}
    {% set project_root = env_var('DBT_PROJECT_ROOT', '../') %}
    CREATE SCHEMA IF NOT EXISTS nba_raw;
    CREATE OR REPLACE VIEW nba_raw.games AS SELECT * FROM read_parquet('{{ project_root }}/data/raw/games/*.parquet');
    CREATE OR REPLACE VIEW nba_raw.player_stats AS SELECT * FROM read_parquet('{{ project_root }}/data/raw/player_stats/*.parquet');
    CREATE OR REPLACE VIEW nba_raw.team_stats AS SELECT * FROM read_parquet('{{ project_root }}/data/raw/team_stats/*.parquet');
{% endmacro %}
