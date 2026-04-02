{#
  Distance Macro Gem
  ==================

  For point-to-point rows, parses WKT POINT columns, computes great-circle
  distance (and optionally cardinal direction and bearing in degrees). When
  types are not both point or no distance/direction outputs are requested,
  returns the relation unchanged.

  Parameters:
    - relation_name (string): Table name; default__ uses backticks.
    - sourceColumnNames (string): Single column name whose values are POINT WKT.
    - destinationColumnNames (string): Same for the destination point column.
    - sourceType / destinationType (string): Both must be 'point' for spatial
      math; otherwise passthrough SELECT *.
    - outputDistance (bool): Emit a distance column (name varies by units).
    - units (string): 'kms' | 'mls' | 'mtr' | 'feet' | other (defaults radius
      naming to generic distance column and km-based earth radius).
    - outputCardDirection (bool): Emit cardinal_direction (N/NE/…).
    - outputDirectionDegrees (bool): Emit direction_degrees (0–360 bearing).
    - allColumnNames (list, default []): Columns to project in the point/point
      branch (backtick-quoted in SELECT); include every column you need on output
      alongside distance/direction fields.

  Adapter Support:
    - Default (backtick-quoted relation/columns; substring parsing of POINT WKT;
      haversine distance; optional bearing via ATAN2/LN)

  Depends on schema parameter:
    No

  Macro Call Examples:
    {{ prophecy_spatial.Distance(
         'pairs',
         'origin_pt',
         'dest_pt',
         'point',
         'point',
         true,
         'kms',
         false,
         false,
         ['id', 'origin_pt', 'dest_pt']
       ) }}

  CTE Usage Example:
    Macro call (example above):
      {{ prophecy_spatial.Distance(
           'pairs',
           'origin_pt',
           'dest_pt',
           'point',
           'point',
           true,
           'kms',
           false,
           false,
           ['id', 'origin_pt', 'dest_pt']
         ) }}

    Resolved query (default__, illustrative fragment — full SQL includes _coords CTE):
      WITH _coords AS (
        SELECT
          `id`,`origin_pt`,`dest_pt`,
          CAST(substring_index(substring_index(`origin_pt`, '(', -1), ' ', 1) AS DOUBLE) AS lon1,
          ...
        FROM `pairs`
      )
      SELECT
        `id`,`origin_pt`,`dest_pt`,
        6371 * 2 * ASIN(...) AS distanceKilometers
      FROM _coords

    For bearing + distance, compile in-project to see _with_bearing and CASE for
    cardinal letters.
#}
{% macro Distance(relation_name,
    sourceColumnNames,
    destinationColumnNames,
    sourceType,
    destinationType,
    outputDistance,
    units,
    outputCardDirection,
    outputDirectionDegrees,
    allColumnNames=[]) -%}
    {{ return(adapter.dispatch('Distance', 'prophecy_spatial')(relation_name,
    sourceColumnNames,
    destinationColumnNames,
    sourceType,
    destinationType,
    outputDistance,
    units,
    outputCardDirection,
    outputDirectionDegrees,
    allColumnNames)) }}
{% endmacro %}


{%- macro default__Distance(
    relation_name,
    sourceColumnNames,
    destinationColumnNames,
    sourceType,
    destinationType,
    outputDistance,
    units,
    outputCardDirection,
    outputDirectionDegrees,
    allColumnNames=[]
) -%}
  {% set cols_str -%}
    {%- for col in allColumnNames -%}
      `{{ col }}`{{ "," if not loop.last }}
    {%- endfor -%}
  {%- endset %}

  {%- if sourceType == 'point'
        and destinationType == 'point'
        and (outputDistance or outputCardDirection or outputDirectionDegrees)
  -%}

    {#–– radius & distance alias ––#}
    {%- if units == 'kms' -%}
      {%- set distance_col = 'distanceKilometers' -%}
      {%- set radius        = 6371 -%}
    {%- elif units == 'mls' -%}
      {%- set distance_col = 'distanceMiles' -%}
      {%- set radius        = 3958.8 -%}
    {%- elif units == 'mtr' -%}
      {%- set distance_col = 'distanceMeters' -%}
      {%- set radius        = 6371000 -%}
    {%- elif units == 'feet' -%}
      {%- set distance_col = 'distanceFeet' -%}
      {%- set radius        = 6371000 * 3.28084 -%}
    {%- else -%}
      {%- set distance_col = 'distance' -%}
      {%- set radius        = 6371 -%}
    {%- endif -%}

    {%- set direction_col = 'cardinal_direction' -%}
    {%- set degrees_col   = 'direction_degrees'   -%}
    {%- set needs_bearing = outputCardDirection or outputDirectionDegrees -%}

    WITH _coords AS (
      SELECT
        {{ cols_str }},
        CAST(
          substring_index(substring_index(`{{ sourceColumnNames }}`, '(', -1), ' ', 1)
        AS DOUBLE) AS lon1,
        CAST(
          substring_index(
            substring_index(substring_index(`{{ sourceColumnNames }}`, '(', -1), ')', 1),
          ' ', -1)
        AS DOUBLE) AS lat1,
        CAST(
          substring_index(substring_index(`{{ destinationColumnNames }}`, '(', -1), ' ', 1)
        AS DOUBLE) AS lon2,
        CAST(
          substring_index(
            substring_index(substring_index(`{{ destinationColumnNames }}`, '(', -1), ')', 1),
          ' ', -1)
        AS DOUBLE) AS lat2
      FROM `{{ relation_name }}`
    )

    {%- if needs_bearing %}
    , _with_bearing AS (
      SELECT
        *,
        MOD(
          DEGREES(
            ATAN2(
              RADIANS(lon2 - lon1),
              LN(
                TAN(RADIANS(lat2)/2 + PI()/4)
                / TAN(RADIANS(lat1)/2 + PI()/4)
              )
            )
          ) + 360,
          360
        ) AS bearing_deg
      FROM _coords
    )

    SELECT
      {{ cols_str }}
      {%- if outputDistance %},
      {{ radius }} * 2 * ASIN(
        SQRT(
          POWER(SIN(RADIANS((lat2 - lat1) / 2)), 2)
          + COS(RADIANS(lat1)) * COS(RADIANS(lat2))
          * POWER(SIN(RADIANS((lon2 - lon1) / 2)), 2)
        )
      ) AS {{ distance_col }}{%- endif %}
      {%- if outputCardDirection %},
      CASE
        WHEN bearing_deg < 22.5 OR bearing_deg >= 337.5 THEN 'N'
        WHEN bearing_deg < 67.5 THEN 'NE'
        WHEN bearing_deg < 112.5 THEN 'E'
        WHEN bearing_deg < 157.5 THEN 'SE'
        WHEN bearing_deg < 202.5 THEN 'S'
        WHEN bearing_deg < 247.5 THEN 'SW'
        WHEN bearing_deg < 292.5 THEN 'W'
        ELSE 'NW'
      END AS {{ direction_col }}{%- endif %}
      {%- if outputDirectionDegrees %},
      bearing_deg AS {{ degrees_col }}{%- endif %}
    FROM _with_bearing

    {%- else %}

      -- only distance requested
      SELECT
        {{ cols_str }},
        {{ radius }} * 2 * ASIN(
          SQRT(
            POWER(SIN(RADIANS((lat2 - lat1) / 2)), 2)
            + COS(RADIANS(lat1)) * COS(RADIANS(lat2))
            * POWER(SIN(RADIANS((lon2 - lon1) / 2)), 2)
          )
        ) AS {{ distance_col }}
      FROM _coords

    {%- endif %}

  {%- else -%}

    SELECT * FROM `{{ relation_name }}`

  {%- endif -%}

{% endmacro %}