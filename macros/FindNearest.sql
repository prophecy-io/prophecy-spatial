{#
  FindNearest Macro Gem
  =====================

  Cross-joins two point tables, ranks targets by haversine distance per source
  row, filters by max distance, and returns the top N neighbors with optional
  column disambiguation. If types are not point-to-point or column names are
  missing, returns the first relation unchanged.

  Parameters:
    - relation_name (list of two strings): [source_table, target_table]; both
      backtick-quoted in default__.
    - source_schema / target_schema (list): Input schema metadata objects
      containing at least `name`; used to project and alias source/target fields.
    - sourceColumnName / destinationColumnName (string): WKT POINT columns on
      source and target; must be non-empty for spatial logic.
    - sourceType / targetType (string): Must both be 'point'.
    - nearestPoints (int, required): ROW_NUMBER cutoff (rn <= nearestPoints).
    - maxDistance (numeric, required): Keep pairs with distance <= maxDistance;
      use 0 to disable the upper bound (WHERE 1=1 on distance).
    - units (string, default 'kms'): Same distance column naming as Distance
      ('kms', 'mls', 'mtr', 'feet', else generic).
    - ignoreZeroDistance (bool, default false): When true, excludes distance = 0.

  Adapter Support:
    - Default (backticks; UUID row id on source; haversine; ROW_NUMBER ordered by distance)

  Depends on schema parameter:
    No

  Macro Call Examples:
    {{ prophecy_spatial.FindNearest(
         ['src_pts', 'tgt_pts'],
         [{"name": "id", "dataType": "string"}, {"name": "geom_src", "dataType": "string"}],
         [{"name": "id", "dataType": "string"}, {"name": "geom_tgt", "dataType": "string"}],
         'geom_src',
         'geom_tgt',
         'point',
         'point',
         3,
         50,
         'kms',
         false
       ) }}

  CTE Usage Example:
    Macro call (example above):
      {{ prophecy_spatial.FindNearest(
           ['src_pts', 'tgt_pts'],
           [{"name": "id", "dataType": "string"}, {"name": "geom_src", "dataType": "string"}],
           [{"name": "id", "dataType": "string"}, {"name": "geom_tgt", "dataType": "string"}],
           'geom_src',
           'geom_tgt',
           'point',
           'point',
           3,
           50,
           'kms',
           false
         ) }}

    Resolved query (default__, illustrative — multiple CTEs; ORDER BY tail):
      ...
      SELECT
        ranked.`id` AS source_id, ranked.target_id,
        rn AS rank_number,
        distanceKilometers,
        CASE ... END AS cardinal_direction
      FROM ranked
      WHERE rn <= 3
      ORDER BY lat1, lon1, rn

    Compile in-project for the full WITH _src, _dst, cross_pts, coords, … chain.
#}
{% macro FindNearest(relation_name,
    source_schema,
    target_schema,
    sourceColumnName,
    destinationColumnName,
    sourceType,
    targetType,
    nearestPoints,
    maxDistance,
    units='kms',
    ignoreZeroDistance=false) -%}
    {{ return(adapter.dispatch('FindNearest', 'prophecy_spatial')(relation_name,
    source_schema,
    target_schema,
    sourceColumnName,
    destinationColumnName,
    sourceType,
    targetType,
    nearestPoints,
    maxDistance,
    units,
    ignoreZeroDistance)) }}
{% endmacro %}

{% macro default__FindNearest(
    relation_name,
    source_schema,
    target_schema,
    sourceColumnName,
    destinationColumnName,
    sourceType,
    targetType,
    nearestPoints,
    maxDistance,
    units='kms',
    ignoreZeroDistance=false
) -%}
  {% set source_column_names = [] -%}
  {% for field in source_schema -%}
    {% do source_column_names.append(field["name"]) %}
  {%- endfor %}
  {% set target_column_names = [] -%}
  {% for field in target_schema -%}
    {% do target_column_names.append(field["name"]) %}
  {%- endfor %}

  {#— Validate required arguments —#}
  {%- if nearestPoints is none %}
    {{ exceptions.raise("FindNearest: 'nearestPoints' must be supplied") }}
  {%- endif %}
  {%- if maxDistance is none %}
    {{ exceptions.raise("FindNearest: 'maxDistance' must be supplied") }}
  {%- endif %}

  {#— Determine radius & distance column name —#}
  {%- if units == 'kms' -%}
    {%- set radius = 6371 -%}
    {%- set distance_col = 'distanceKilometers' -%}
  {%- elif units == 'mls' -%}
    {%- set radius = 3958.8 -%}
    {%- set distance_col = 'distanceMiles' -%}
  {%- elif units == 'mtr' -%}
    {%- set radius = 6371000 -%}
    {%- set distance_col = 'distanceMeters' -%}
  {%- elif units == 'feet' -%}
    {%- set radius = 6371000 * 3.28084 -%}
    {%- set distance_col = 'distanceFeet' -%}
  {%- else -%}
    {%- set radius = 6371 -%}
    {%- set distance_col = 'distance' -%}
  {%- endif -%}

  {#— Build SELECT-list for source columns, aliasing conflicts —#}
  {%- set src_select_list = [] -%}
  {%- set src_cols_no_alias = [] -%}
  {%- for c in source_column_names %}
    {%- do src_cols_no_alias.append('`' ~ c ~ '`') -%}
    {%- if c in target_column_names %}
      {%- do src_select_list.append('s.`' ~ c ~ '` AS source_' ~ c) -%}
    {%- else %}
      {%- do src_select_list.append('s.`' ~ c ~ '`') -%}
    {%- endif %}
  {%- endfor %}
  {%- set src_cols_no_alias_str = src_cols_no_alias | join(', ') -%}
  {%- set src_select_str = src_select_list | join(', ') -%}

  {#— Build SELECT-list for target columns, aliasing conflicts —#}
  {%- set tgt_select_list = [] -%}
  {%- set tgt_cols_no_alias = [] -%}
  {%- for c in target_column_names %}
    {%- do tgt_cols_no_alias.append('`' ~ c ~ '`') -%}
    {%- if c in source_column_names %}
      {%- do tgt_select_list.append('d.`' ~ c ~ '` AS target_' ~ c) -%}
    {%- else %}
      {%- do tgt_select_list.append('d.`' ~ c ~ '`') -%}
    {%- endif %}
  {%- endfor %}
  {%- set tgt_cols_no_alias_str = tgt_cols_no_alias | join(', ') -%}
  {%- set tgt_select_str = tgt_select_list | join(', ') -%}

  {#— Proceed only if both are points and column names provided —#}
  {%- if
        sourceType == 'point'
    and targetType == 'point'
    and sourceColumnName   != ''
    and destinationColumnName != ''
  -%}

    WITH
    _src AS (
      SELECT UUID() AS s_rowid, {{ src_cols_no_alias_str }}
      FROM `{{ relation_name[0] }}`
    ),
    _dst AS (
      SELECT {{ tgt_cols_no_alias_str }}
      FROM `{{ relation_name[1] }}`
    ),

    cross_pts AS (
      SELECT
        s_rowid,
        {{ src_select_str }}{% if src_select_str and tgt_select_str %}, {% endif %}{{ tgt_select_str }},
        s.`{{ sourceColumnName }}`   AS src_point,
        d.`{{ destinationColumnName }}` AS dst_point
      FROM _src s
      CROSS JOIN _dst d
    ),

    coords AS (
      SELECT
        *,
        CAST(
          SUBSTRING_INDEX(SUBSTRING_INDEX(src_point, '(', -1), ' ', 1)
        AS DOUBLE) AS lon1,
        CAST(
          SUBSTRING_INDEX(
            SUBSTRING_INDEX(SUBSTRING_INDEX(src_point, '(', -1), ')', 1),
            ' ', -1
          )
        AS DOUBLE) AS lat1,
        CAST(
          SUBSTRING_INDEX(SUBSTRING_INDEX(dst_point, '(', -1), ' ', 1)
        AS DOUBLE) AS lon2,
        CAST(
          SUBSTRING_INDEX(
            SUBSTRING_INDEX(SUBSTRING_INDEX(dst_point, '(', -1), ')', 1),
            ' ', -1
          )
        AS DOUBLE) AS lat2
      FROM cross_pts
    ),

    with_bearing AS (
      SELECT
        *,
        MOD(
          DEGREES(
            ATAN2(
              RADIANS(lon2 - lon1),
              LN(
                TAN(RADIANS(lat2) / 2 + PI() / 4)
                / TAN(RADIANS(lat1) / 2 + PI() / 4)
              )
            )
          ) + 360,
          360
        ) AS bearing_deg
      FROM coords
    ),

    distances AS (
      SELECT
        *,
        {{ radius }} * 2 * ASIN(
          SQRT(
            POWER(SIN(RADIANS((lat2 - lat1) / 2)), 2)
            + COS(RADIANS(lat1)) * COS(RADIANS(lat2))
            * POWER(SIN(RADIANS((lon2 - lon1) / 2)), 2)
          )
        ) AS {{ distance_col }}
      FROM with_bearing
    ),

    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY s_rowid
          ORDER BY {{ distance_col }} ASC
        ) AS rn
      FROM distances
      WHERE
        {%- if maxDistance == 0 %}
        1=1
        {%- else %}
        {{ distance_col }} <= {{ maxDistance }}
        {%- endif -%}
        {%- if ignoreZeroDistance %} AND {{ distance_col }} <> 0{%- endif -%}
    )

    SELECT
      {#— Final source columns (qualified) —#}
      {%- set final_src_list = [] -%}
      {%- for c in source_column_names %}
        {%- if c in target_column_names %}
          {%- do final_src_list.append('ranked.source_' ~ c ~ ' AS source_' ~ c) -%}
        {%- else %}
          {%- do final_src_list.append('ranked.`' ~ c ~ '`') -%}
        {%- endif %}
      {%- endfor %}
      {%- set final_src_str = final_src_list | join(', ') -%}

      {#— Final target columns (qualified) —#}
      {%- set final_tgt_list = [] -%}
      {%- for c in target_column_names %}
        {%- if c in source_column_names %}
          {%- do final_tgt_list.append('ranked.target_' ~ c ~ ' AS target_' ~ c) -%}
        {%- else %}
          {%- do final_tgt_list.append('ranked.`' ~ c ~ '`') -%}
        {%- endif %}
      {%- endfor %}
      {%- set final_tgt_str = final_tgt_list | join(', ') -%}

      {{ final_src_str }}{% if final_src_str and final_tgt_str %}, {% endif %}{{ final_tgt_str }},
      rn AS rank_number,
      {{ distance_col }},
      CASE
        WHEN bearing_deg < 22.5 OR bearing_deg >= 337.5 THEN 'N'
        WHEN bearing_deg < 67.5 THEN 'NE'
        WHEN bearing_deg < 112.5 THEN 'E'
        WHEN bearing_deg < 157.5 THEN 'SE'
        WHEN bearing_deg < 202.5 THEN 'S'
        WHEN bearing_deg < 247.5 THEN 'SW'
        WHEN bearing_deg < 292.5 THEN 'W'
        ELSE 'NW'
      END AS cardinal_direction
    FROM ranked
    WHERE rn <= {{ nearestPoints }}
    ORDER BY lat1, lon1, rn

  {%- else -%}

    -- If not point→point (or missing column names), return source table as-is
    SELECT * FROM `{{ relation_name[0] }}`

  {%- endif -%}

{%- endmacro %}

{%- macro snowflake__FindNearest(
    relation_name,
    source_schema,
    target_schema,
    sourceColumnName,
    destinationColumnName,
    sourceType,
    targetType,
    nearestPoints,
    maxDistance,
    units='kms',
    ignoreZeroDistance=false
) -%}

{% set src_relation = relation_name[0] %}
{% set tgt_relation = relation_name[1] %}

{% set src_geom = prophecy_basics.quote_identifier(sourceColumnName) %}
{% set tgt_geom = prophecy_basics.quote_identifier(destinationColumnName) %}

{%- if units == 'kms' -%}
  {%- set divisor = 1000 -%}
  {%- set distance_col = 'distance_km' -%}
{%- elif units == 'mls' -%}
  {%- set divisor = 1609.34 -%}
  {%- set distance_col = 'distance_miles' -%}
{%- elif units == 'mtr' -%}
  {%- set divisor = 1 -%}
  {%- set distance_col = 'distance_meters' -%}
{%- elif units == 'feet' -%}
  {%- set divisor = 0.3048 -%}
  {%- set distance_col = 'distance_feet' -%}
{%- else -%}
  {%- set divisor = 1000 -%}
  {%- set distance_col = 'distance' -%}
{%- endif -%}

{% set src_cols = [] %}
{% set tgt_cols = [] %}

{% set src_names = source_schema | map(attribute='name') | list %}
{% set tgt_names = target_schema | map(attribute='name') | list %}

{% for c in src_names %}
  {% if c in tgt_names %}
    {% do src_cols.append('s.' ~ prophecy_basics.quote_identifier(c) ~ ' AS source_' ~ c) %}
  {% else %}
    {% do src_cols.append('s.' ~ prophecy_basics.quote_identifier(c)) %}
  {% endif %}
{% endfor %}

{% for c in tgt_names %}
  {% if c in src_names %}
    {% do tgt_cols.append('d.' ~ prophecy_basics.quote_identifier(c) ~ ' AS target_' ~ c) %}
  {% else %}
    {% do tgt_cols.append('d.' ~ prophecy_basics.quote_identifier(c)) %}
  {% endif %}
{% endfor %}

{% set src_select = src_cols | join(', ') %}
{% set tgt_select = tgt_cols | join(', ') %}

{%- if sourceType == 'point' and targetType == 'point'
      and sourceColumnName != '' and destinationColumnName != '' -%}

WITH src AS (
    SELECT
        SEQ8() AS s_rowid,
        {{ src_select }},
        TRY_TO_GEOGRAPHY({{ src_geom }}) AS src_geo
    FROM {{ src_relation }}
),

dst AS (
    SELECT
        {{ tgt_select }},
        TRY_TO_GEOGRAPHY({{ tgt_geom }}) AS dst_geo
    FROM {{ tgt_relation }}
),

joined AS (
    SELECT
        s.*,
        d.*,
        ST_DISTANCE(s.src_geo, d.dst_geo) / {{ divisor }} AS {{ distance_col }}
    FROM src s
    CROSS JOIN dst d
),

filtered AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY s_rowid
            ORDER BY {{ distance_col }}
        ) AS rn
    FROM joined
    WHERE
        src_geo IS NOT NULL
        AND dst_geo IS NOT NULL
        {%- if maxDistance != 0 %}
        AND {{ distance_col }} <= {{ maxDistance }}
        {%- endif %}
        {%- if ignoreZeroDistance %}
        AND {{ distance_col }} <> 0
        {%- endif %}
)

SELECT
    *,
    rn AS rank_number
FROM filtered
WHERE rn <= {{ nearestPoints }}

{%- else -%}

SELECT * FROM {{ src_relation }}

{%- endif -%}

{%- endmacro %}