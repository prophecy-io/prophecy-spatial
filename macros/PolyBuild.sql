{#
  PolyBuild Macro Gem
  ===================

  Groups rows and builds one WKT polygon or linestring per group by ordering
  coordinate pairs (optionally by a sequence column). If lon/lat names are
  missing, returns all rows unchanged.

  Parameters:
    - relation_name (string): Source relation; adapter.quote not applied to the
      relation itself in default__ (pass identifier as your engine expects).
    - buildMethod (string): Compared lowercased to 'sequencepolygon' vs
      'sequencepolyline' to choose POLYGON((...)) vs LINESTRING(...).
    - longitudeColumnName / latitudeColumnName (string): Required for build;
      adapter.quote applied; empty/whitespace → SELECT * passthrough.
    - groupColumnName (string, default ''): If non-empty, groups by this column;
      else a constant grouping key of 1.
    - sequenceColumnName (string, default ''): If non-empty, prepended into the
      sort key so vertices order within each group.

  Adapter Support:
    - Default (Spark collect_list / sort_array / struct; concat_ws; element_at to close rings)

  Depends on schema parameter:
    No

  Macro Call Examples:
    {{ prophecy_spatial.PolyBuild('tracks', 'sequencepolyline', 'lon', 'lat', 'route_id', 'seq') }}

  CTE Usage Example:
    Macro call (example above):
      {{ prophecy_spatial.PolyBuild('tracks', 'sequencepolyline', 'lon', 'lat', 'route_id', 'seq') }}

    Resolved query (default__, illustrative WITH shape):
      WITH coords AS (
        SELECT
          <quoted route_id> AS grouping_column_name,
          CONCAT(<quoted seq>, <quoted lon>, <quoted lat>) AS sequencing_column_name,
          <quoted lon> AS lon,
          <quoted lat> AS lat,
          CONCAT(CAST(<quoted lon> AS STRING), ' ', CAST(<quoted lat> AS STRING)) AS coord
        FROM tracks
      ),
      ordered AS (...),
      verts AS (...)
      SELECT
        grouping_column_name,
        CASE
          WHEN 'sequencepolyline' = 'sequencepolygon' THEN
            CONCAT('POLYGON((', concat_ws(', ', v), ', ', element_at(v, 1), '))')
          ELSE
            CONCAT('LINESTRING(', concat_ws(', ', v), ')')
        END AS geometry_wkt
      FROM verts

    Quoting follows adapter.quote for lon/lat/group/sequence; compile in-project for exact SQL.
#}
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