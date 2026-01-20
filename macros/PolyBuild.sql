{% macro PolyBuild(relation_name,
        buildMethod,
        longitudeColumnName,
        latitudeColumnName,
        groupColumnName='',
        sequenceColumnName='') -%}
    {{ return(adapter.dispatch('PolyBuild', 'prophecy_spatial')(relation_name,
        buildMethod,
        longitudeColumnName,
        latitudeColumnName,
        groupColumnName,
        sequenceColumnName)) }}
{% endmacro %}

{% macro default__PolyBuild(
        relation_name,
        buildMethod,
        longitudeColumnName,
        latitudeColumnName,
        groupColumnName='',
        sequenceColumnName=''
) %}

{# ── 0. quick passthrough check ────────────────────────────────────────── #}
{% if longitudeColumnName | trim | length == 0
      or latitudeColumnName  | trim | length == 0 %}
    {{ log('PolyBuild: lon/lat column missing → returning raw rows.', info=True) }}
    SELECT * FROM {{ relation_name }}
{% else %}
    {# ── validate buildMethod ──────────────────────────────────────────────── #}
    {% set method = buildMethod | lower %}
    {# ── flag presence of group / sequence columns ─────────────────────────── #}
    {% set has_group = groupColumnName   | trim | length > 0 %}
    {% set has_seq   = sequenceColumnName | trim | length > 0 %}

    {# ── pre-quote column names once ───────────────────────────────────────── #}
    {% set lon = adapter.quote(longitudeColumnName) %}
    {% set lat = adapter.quote(latitudeColumnName) %}
    {% if has_group %}{% set grp = adapter.quote(groupColumnName) %}{% endif %}
    {% if has_seq  %}{% set seq = adapter.quote(sequenceColumnName) %}{% endif %}

    WITH coords AS (

        SELECT
            {# group key #}
            {% if has_group -%}
                {{ grp }} AS grouping_column_name,
            {%- else -%}
                1 AS grouping_column_name,
            {%- endif %}

            {# sequence key #}
            {% if has_seq -%}
                CONCAT({{ seq }}, {{ lon }}, {{ lat }}) AS sequencing_column_name,
            {%- else -%}
                CONCAT({{ lon }}, {{ lat }})            AS sequencing_column_name,
            {%- endif %}

            {{ lon }} AS lon,
            {{ lat }} AS lat,
            CONCAT(CAST({{ lon }} AS STRING), ' ', CAST({{ lat }} AS STRING)) AS coord
        FROM {{ relation_name }}

    ), ordered AS (

        SELECT
            grouping_column_name,
            sort_array(collect_list(struct(sequencing_column_name, coord))) AS ordered_coords
        FROM coords
        GROUP BY grouping_column_name

    ), verts AS (

        SELECT
            grouping_column_name,
            transform(ordered_coords, x -> x.coord) AS v
        FROM ordered

    )

    SELECT
        {% if has_group %}
            grouping_column_name,
        {% endif %}
        CASE
            WHEN '{{ method }}' = 'sequencepolygon'
                 THEN CONCAT(
                        'POLYGON((',
                        concat_ws(', ', v),
                        ', ',
                        element_at(v, 1),   -- close ring
                        '))'
                      )
            ELSE  /* 'sequencepolyline' */
                 CONCAT(
                        'LINESTRING(',
                        concat_ws(', ', v),
                        ')'
                      )
        END AS geometry_wkt
    FROM verts
{% endif %}
{% endmacro %}