{% macro HeatMap(relation_name,
        longitudeColumnName,
        latitudeColumnName,
        resolution,
        gridDistance,
        heatColumnName = none,
        decayType      = 'constant') -%}
    {{ return(adapter.dispatch('HeatMap', 'prophecy_spatial')(relation_name,
        longitudeColumnName,
        latitudeColumnName,
        resolution,
        gridDistance,
        heatColumnName,
        decayType)) }}
{% endmacro %}

{%- macro default__HeatMap(
        relation_name,
        longitudeColumnName,
        latitudeColumnName,
        resolution,
        gridDistance,
        heatColumnName = none,
        decayType      = 'constant'
    ) -%}

{# ── 0. quick passthrough check ─────────────────────────────────────────────── #}
{%- if longitudeColumnName | trim | length == 0
      or latitudeColumnName  | trim | length == 0 -%}
    SELECT * FROM {{ relation_name }}

{%- else -%}

{# ── 1. decide per-point heat expression (1 if column not provided) ─────────── #}
{%- if heatColumnName is not none and heatColumnName | trim | length > 0 -%}
    {%- set heat_expr = heatColumnName -%}
{%- else -%}
    {%- set heat_expr = '1' -%}
{%- endif -%}

{# normalise decayType once for SQL CASEs #}
{%- set decay = (decayType | lower | trim) or 'constant' -%}

-- ── Hex-bin & weighted / decayed density ─────────────────────────────────────
WITH points_h3 AS (
    SELECT
        h3_longlatash3(
            {{ longitudeColumnName }},
            {{ latitudeColumnName }},
            {{ resolution }}
        )                            AS h3_cell,
        {{ heat_expr }}              AS point_heat
    FROM {{ relation_name }}
),

cell_counts AS (
    -- raw (weighted) heat per hex
    SELECT
        h3_cell,
        SUM(point_heat) AS raw_heat
    FROM points_h3
    GROUP BY h3_cell
),

cell_counts_smoothed AS (
    -- k-ring smoothing + decay kernel
    SELECT
        neighbour AS h3_cell,
        SUM(
            c.raw_heat *
            CASE
                {% if gridDistance == 0 %}
                    WHEN true THEN 1     -- no smoothing requested
                {% else %}
                    {% if decay == 'constant' %}
                        WHEN true THEN 1
                    {% elif decay == 'linear' %}
                        -- 1 − d / k + 1  (outer ring gets a small, non-zero share)
                        WHEN true THEN (1 - (h3_distance(c.h3_cell, neighbour) / ({{ gridDistance }} + 1)))
                    {% elif decay == 'exp' %}
                        -- halves each ring: 0.5^d
                        WHEN true THEN POWER(0.5, h3_distance(c.h3_cell, neighbour))
                    {% else %}  {# exponential #}
                        -- halves each ring: 0.5^d
                        WHEN true THEN 1
                    {% endif %}
                {% endif %}
            END
        ) AS density
    FROM cell_counts AS c
    LATERAL VIEW explode(
        CASE
            WHEN {{ gridDistance }} = 0
                 THEN array(c.h3_cell)
                 ELSE h3_kring(c.h3_cell, {{ gridDistance }})
        END
    ) t AS neighbour
    GROUP BY neighbour
)

SELECT
    round(density,2) as density,
    h3_boundaryaswkt(h3_cell) AS geometry_wkt
FROM cell_counts_smoothed

{%- endif -%}
{%- endmacro -%}
