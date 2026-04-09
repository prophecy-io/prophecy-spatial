{#
  Simplify Macro Gem
  ==================

  Simplifies per-row geometry (Douglas-Peucker-style tolerance in Web
  Mercator meters) and returns the result as WKT in WGS84. Use to reduce vertex
  count for lighter maps or downstream spatial joins.

  Parameters:
    - relation_name (list): One-element list naming the relation used in FROM (same
      role as relation_name elsewhere), e.g. ['boundaries']; default__ uses that
      name passed through to SQL.
    - schema (string): Logged in default__ only; does not affect generated SQL.
    - geom_column_name (string): WKT column; ST_GeomFromText(..., 4326).
    - tolerance (numeric): Simplification tolerance in kilometers if unit is
      "kilometers", else miles (converted to meters via ×1609.34).
    - unit (string): "kilometers" → tolerance×1000 meters; otherwise miles.

  Adapter Support:
    - Default (Spark/Databricks-style ST_Simplify / ST_Transform 4326↔3857)

  Depends on schema parameter:
    No

  Macro Call Examples:
    {{ prophecy_spatial.Simplify(['boundaries'], 'staging', 'geom_wkt', 0.1, 'kilometers') }}

  CTE Usage Example:
    Macro call (example above):
      {{ prophecy_spatial.Simplify(['boundaries'], 'staging', 'geom_wkt', 0.1, 'kilometers') }}

    Resolved query (default__):
      SELECT
        geom_wkt as input,
        ST_AsText(
          ST_Transform(
            ST_Simplify(
              ST_Transform(
                ST_GeomFromText(geom_wkt, 4326),
                3857
              ),
              100
            ),
            4326
          )
        ) as output
      FROM
        boundaries
#}
{% macro Simplify(relation_name, schema, geom_column_name, tolerance, unit) -%}
    {{ return(adapter.dispatch('Simplify', 'prophecy_spatial')(relation_name, schema, geom_column_name, tolerance, unit)) }}
{% endmacro %}

{%- macro default__Simplify(relation_name, schema, geom_column_name, tolerance, unit) -%}
  {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
  {{ log("relation_name=" ~ relation_name, info=True) }}
  {{ log("schema=" ~ schema, info=True) }}
  {{ log("geom_column_name=" ~ geom_column_name, info=True) }}
  {{ log("tolerance=" ~ tolerance, info=True) }}
  {{ log("unit=" ~ unit, info=True) }}

  {%- if unit == 'kilometers' -%}
    {%- set tolerance_meters = tolerance * 1000 -%}
  {%- else -%}
    {%- set tolerance_meters = tolerance * 1609.34 -%}
  {%- endif -%}

  SELECT
    {{geom_column_name}} as input,
    ST_AsText(
      ST_Transform(
        ST_Simplify(
          ST_Transform(
            ST_GeomFromText({{geom_column_name}}, 4326),
            3857
          ),
          {{tolerance_meters}}
        ),
        4326
      )
    ) as output
  FROM
    {{ relation_list | join(', ') }}

{%- endmacro -%}
