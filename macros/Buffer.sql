{#
  Buffer Macro Gem
  ================

  Expands per-row geometry by a fixed distance around the shape (computed in
  Web Mercator, then returned as WKT in WGS84). Use for proximity zones,
  search radii, or thickened boundaries around lines and polygons.

  Parameters:
    - relation_name (list): One-element list naming the relation used in FROM (same
      role as relation_name elsewhere), e.g. ['roads']; default__ uses that name
      passed through to SQL (add quoting/qualifiers as required by your warehouse).
    - schema (string): Logged in default__ only; does not affect generated SQL.
    - geometryColumnName (string): Column holding WKT text; fed to ST_GeomFromText(..., 4326).
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
{% macro Buffer(relation_name, schema, geometryColumnName, distance, unit) -%}
    {{ return(adapter.dispatch('Buffer', 'prophecy_spatial')(relation_name, schema, geometryColumnName, distance, unit)) }}
{% endmacro %}


{%- macro default__Buffer(
        relation_name, schema, geometryColumnName, distance, unit
) -%}
  {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}


  {%- if unit == 'kilometers' -%}
    {%- set distance_meters = distance * 1000 -%}
  {%- else -%}
    {%- set distance_meters = distance * 1609.34 -%}
  {%- endif -%}

  SELECT
    {{geometryColumnName}} as input,
    ST_AsText(
      ST_Transform(
        ST_Buffer(
          ST_Transform(
            ST_GeomFromText({{geometryColumnName}}, 4326),
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

{%- macro snowflake__Buffer(
    relation_name,
    schema,
    geometryColumnName,
    distance,
    unit
) -%}

{# Normalize relation input #}
{% set relation_list = [] %}
{% for r in (relation_name if relation_name is iterable and relation_name is not string else [relation_name]) %}
  {% if r is string %}
    {% set r2 = r | replace('[','') | replace(']','') | replace('"','') | replace("'","") %}
    {% do relation_list.append(r2) %}
  {% else %}
    {% do relation_list.append(r) %}
  {% endif %}
{% endfor %}

{# Convert distance → degrees (since GEOMETRY buffer uses degrees for lat/lon) #}
{%- if unit == 'kilometers' -%}
  {%- set distance_degrees = (distance * 1000) / 111320 -%}
{%- elif unit == 'miles' -%}
  {%- set distance_degrees = (distance * 1609.34) / 111320 -%}
{%- else -%}
  {# assume meters #}
  {%- set distance_degrees = distance / 111320 -%}
{%- endif -%}

{% set geom_col = prophecy_basics.quote_identifier(geometryColumnName) %}

SELECT
  {{ geom_col }} AS input,
  ST_ASWKT(
    ST_BUFFER(
      TRY_TO_GEOMETRY({{ geom_col }}),
      ({{ distance_degrees }})::FLOAT
    )
  ) AS output
FROM {{ relation_list | join(', ') }}

{%- endmacro %}