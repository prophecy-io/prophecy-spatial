{#
  Buffer Macro Gem
  ================

  Expands per-row geometry by a fixed distance around the shape (computed in
  Web Mercator, then returned as WKT in WGS84). Use for proximity zones,
  search radii, or thickened boundaries around lines and polygons.

  Parameters:
    - table_name (list): One-element list naming the relation used in FROM (same
      role as relation_name elsewhere), e.g. ['roads']; default__ uses that name
      passed through to SQL (add quoting/qualifiers as required by your warehouse).
    - schema (string): Logged in default__ only; does not affect generated SQL.
    - geom_column_name (string): Column holding WKT text; fed to ST_GeomFromText(..., 4326).
    - distance (numeric): Buffer distance in kilometers when unit is
      "kilometers", otherwise treated as miles and converted to meters
      (×1609.34).
    - unit (string): "kilometers" uses distance×1000 as meters; any other value
      uses miles-to-meters conversion.

  Adapter Support:
    - Default (Spark/Databricks-style ST_*; ST_Transform 4326↔3857, ST_Buffer in meters)

  Depends on schema parameter:
    No

  Macro Call Examples:
    {{ prophecy_spatial.Buffer(['roads'], 'staging', 'geom_wkt', 0.5, 'kilometers') }}

  CTE Usage Example:
    Macro call (example above):
      {{ prophecy_spatial.Buffer(['roads'], 'staging', 'geom_wkt', 0.5, 'kilometers') }}

    Resolved query (default__):
      SELECT
        geom_wkt as input,
        ST_AsText(
          ST_Transform(
            ST_Buffer(
              ST_Transform(
                ST_GeomFromText(geom_wkt, 4326),
                3857
              ),
              500
            ),
            4326
          )
        ) as output
      FROM
        roads
#}
{% macro Buffer(table_name, schema, geom_column_name, distance, unit) -%}
    {{ return(adapter.dispatch('Buffer', 'prophecy_spatial')(table_name, schema, geom_column_name, distance, unit)) }}
{% endmacro %}


{%- macro default__Buffer(
        table_name, schema, geom_column_name, distance, unit
) -%}
  {% set relation_list = table_name if table_name is iterable and table_name is not string else [table_name] %}


  {%- if unit == 'kilometers' -%}
    {%- set distance_meters = distance * 1000 -%}
  {%- else -%}
    {%- set distance_meters = distance * 1609.34 -%}
  {%- endif -%}

  SELECT
    {{geom_column_name}} as input,
    ST_AsText(
      ST_Transform(
        ST_Buffer(
          ST_Transform(
            ST_GeomFromText({{geom_column_name}}, 4326),
            3857
          ),
          {{distance_meters}}
        ),
        4326
      )
    ) as output
  FROM
    {{ relation_list | join(', ') }}

{%- endmacro -%}
