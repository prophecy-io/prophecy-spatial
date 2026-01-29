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

{%- macro duckdb__Distance(
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
          string_split(string_split(`{{ sourceColumnNames }}`, '(')[2], ' ')[1]
        AS DOUBLE) AS lon1,
        CAST(
          string_split(string_split(string_split(`{{ sourceColumnNames }}`, '(')[2], ')')[1], ' ')[2]
        AS DOUBLE) AS lat1,
        CAST(
          string_split(string_split(`{{ destinationColumnNames }}`, '(')[2], ' ')[1]
        AS DOUBLE) AS lon2,
        CAST(
          string_split(string_split(string_split(`{{ destinationColumnNames }}`, '(')[2], ')')[1], ' ')[2]
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

{%- macro bigquery__Distance(
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
  {%- set cols_str = prophecy_basics.quote_column_list(allColumnNames | join(', ')) -%}

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
          SPLIT(REPLACE(REPLACE({{ prophecy_basics.quote_identifier(sourceColumnNames) }}, 'POINT (', ''), ')', ''), ' ')[SAFE_OFFSET(0)]
        AS FLOAT64) AS lon1,
        CAST(
          SPLIT(REPLACE(REPLACE({{ prophecy_basics.quote_identifier(sourceColumnNames) }}, 'POINT (', ''), ')', ''), ' ')[SAFE_OFFSET(1)]
        AS FLOAT64) AS lat1,
        CAST(
          SPLIT(REPLACE(REPLACE({{ prophecy_basics.quote_identifier(destinationColumnNames) }}, 'POINT (', ''), ')', ''), ' ')[SAFE_OFFSET(0)]
        AS FLOAT64) AS lon2,
        CAST(
          SPLIT(REPLACE(REPLACE({{ prophecy_basics.quote_identifier(destinationColumnNames) }}, 'POINT (', ''), ')', ''), ' ')[SAFE_OFFSET(1)]
        AS FLOAT64) AS lat2
      FROM {{ prophecy_basics.quote_identifier(relation_name) }}
    )

    {%- if needs_bearing %}
    , _with_bearing AS (
      SELECT
        *,
        (
          ATAN2(
            (lon2 - lon1) * ACOS(-1) / 180,
            LN(
              TAN((lat2) * ACOS(-1) / 180 / 2 + ACOS(-1)/4)
              / TAN((lat1) * ACOS(-1) / 180 / 2 + ACOS(-1)/4)
            )
          ) * 180 / ACOS(-1) + 360
        ) - FLOOR((
          ATAN2(
            (lon2 - lon1) * ACOS(-1) / 180,
            LN(
              TAN((lat2) * ACOS(-1) / 180 / 2 + ACOS(-1)/4)
              / TAN((lat1) * ACOS(-1) / 180 / 2 + ACOS(-1)/4)
            )
          ) * 180 / ACOS(-1) + 360
        ) / 360) * 360 AS bearing_deg
      FROM _coords
    )

    SELECT
      {{ cols_str }}
      {%- if outputDistance %},
      {{ radius }} * 2 * ASIN(
        SQRT(
          POWER(SIN(((lat2 - lat1) / 2) * ACOS(-1) / 180), 2)
          + COS(lat1 * ACOS(-1) / 180) * COS(lat2 * ACOS(-1) / 180)
          * POWER(SIN(((lon2 - lon1) / 2) * ACOS(-1) / 180), 2)
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
            POWER(SIN(((lat2 - lat1) / 2) * ACOS(-1) / 180), 2)
            + COS(lat1 * ACOS(-1) / 180) * COS(lat2 * ACOS(-1) / 180)
            * POWER(SIN(((lon2 - lon1) / 2) * ACOS(-1) / 180), 2)
          )
        ) AS {{ distance_col }}
      FROM _coords

    {%- endif %}

  {%- else -%}

    SELECT * FROM {{ prophecy_basics.quote_identifier(relation_name) }}

  {%- endif -%}

{%- endmacro %}