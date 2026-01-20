{% macro FindNearest(relation_names,
    sourceColumnName,
    destinationColumnName,
    sourceType,
    destinationType,
    nearestPoints,
    maxDistance,
    units='kms',
    ignoreZeroDistance=false,
    allSourceColumnNames=[],
    allTargetColumnNames=[]) -%}
    {{ return(adapter.dispatch('FindNearest', 'prophecy_spatial')(relation_names,
    sourceColumnName,
    destinationColumnName,
    sourceType,
    destinationType,
    nearestPoints,
    maxDistance,
    units,
    ignoreZeroDistance,
    allSourceColumnNames,
    allTargetColumnNames)) }}
{% endmacro %}

{% macro default__FindNearest(
    relation_names,
    sourceColumnName,
    destinationColumnName,
    sourceType,
    destinationType,
    nearestPoints,
    maxDistance,
    units='kms',
    ignoreZeroDistance=false,
    allSourceColumnNames=[],
    allTargetColumnNames=[]
) -%}

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
  {%- for c in allSourceColumnNames %}
    {%- do src_cols_no_alias.append('`' ~ c ~ '`') -%}
    {%- if c in allTargetColumnNames %}
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
  {%- for c in allTargetColumnNames %}
    {%- do tgt_cols_no_alias.append('`' ~ c ~ '`') -%}
    {%- if c in allSourceColumnNames %}
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
    and destinationType == 'point'
    and sourceColumnName   != ''
    and destinationColumnName != ''
  -%}

    WITH
    _src AS (
      SELECT UUID() AS s_rowid, {{ src_cols_no_alias_str }}
      FROM `{{ relation_names[0] }}`
    ),
    _dst AS (
      SELECT {{ tgt_cols_no_alias_str }}
      FROM `{{ relation_names[1] }}`
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
      {%- for c in allSourceColumnNames %}
        {%- if c in allTargetColumnNames %}
          {%- do final_src_list.append('ranked.source_' ~ c ~ ' AS source_' ~ c) -%}
        {%- else %}
          {%- do final_src_list.append('ranked.`' ~ c ~ '`') -%}
        {%- endif %}
      {%- endfor %}
      {%- set final_src_str = final_src_list | join(', ') -%}

      {#— Final target columns (qualified) —#}
      {%- set final_tgt_list = [] -%}
      {%- for c in allTargetColumnNames %}
        {%- if c in allSourceColumnNames %}
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
    SELECT * FROM `{{ relation_names[0] }}`

  {%- endif -%}

{%- endmacro %}
