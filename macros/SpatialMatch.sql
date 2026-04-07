{#
  SpatialMatch Macro Gem
  ======================

  Spatially joins two relations on WKT geometry columns: cross join filtered by
  ST_* predicates (intersects, contains, within, touches, combined touches OR
  intersects, or envelope intersection). Output columns are driven by two column
  lists—source columns keep plain names, target columns are prefixed target_.

  Parameters:
    - relation_name (list of two strings): [source_relation, target_relation];
      used in FROM ... AS source / AS target (no extra quoting in default__).
    - schemas (list of two lists of strings): schemas[0] = source column names to
      select; schemas[1] = target column names (aliased as target_<name>). Order
      and membership of output columns follow these lists.
    - source_column / target_column (string): Geometry column names on each side for
      ST_GeomFromText(source.<col>) / ST_GeomFromText(target.<col>).
    - match_type (string): 'intersects' | 'contains' | 'within' | 'touches' |
      'touches_or_intersects' | 'envelope'; unknown types fall through to WHERE 1=1.

  Adapter Support:
    - Default (unqualified relation names; ST_GeomFromText without SRID; no adapter-specific overrides in-repo)

  Depends on schema parameter:
    Yes

  Macro Call Examples:
    {{ prophecy_spatial.SpatialMatch(
         ['places', 'regions'],
         [['id', 'geom_wkt'], ['id', 'geom_wkt']],
         'geom_wkt',
         'geom_wkt',
         'intersects'
       ) }}

  CTE Usage Example:
    Macro call (example above):
      {{ prophecy_spatial.SpatialMatch(
           ['places', 'regions'],
           [['id', 'geom_wkt'], ['id', 'geom_wkt']],
           'geom_wkt',
           'geom_wkt',
           'intersects'
         ) }}

    Resolved query (default__):
      SELECT
        source.id,
        source.geom_wkt,
        target.id AS target_id,
        target.geom_wkt AS target_geom_wkt
      FROM places AS source
      CROSS JOIN regions AS target
      WHERE
        ST_Intersects(
          ST_GeomFromText(source.geom_wkt),
          ST_GeomFromText(target.geom_wkt)
        )
#}
{% macro SpatialMatch(relation_name,
    schemas,
    source_column,
    target_column,
    match_type) -%}
    {{ return(adapter.dispatch('SpatialMatch', 'prophecy_spatial')(relation_name,
    schemas,
    source_column,
    target_column,
    match_type)) }}
{% endmacro %}

{% macro default__SpatialMatch(
    relation_name,
    schemas,
    source_column,
    target_column,
    match_type
) -%}

  {% set fn_map = {
    'intersects': 'ST_Intersects',
    'contains': 'ST_Contains',
    'within': 'ST_Within',
    'touches': 'ST_Touches'
  } %}

  {% set source_relation = relation_name[0] %}
  {% set target_relation = relation_name[1] %}

  {% set source_columns = schemas[0] %}
  {% set target_columns = schemas[1] %}

  {% set source_select = [] %}
  {% for col in source_columns %}
    {% do source_select.append('source.' ~ col) %}
  {% endfor %}

  {% set target_select = [] %}
  {% for col in target_columns %}
    {% do target_select.append('target.' ~ col ~ ' AS target_' ~ col) %}
  {% endfor %}

  {% set spatial_fn = fn_map.get(match_type) %}

  SELECT
    {{ (source_select + target_select) | join(',\n    ') }}
  FROM {{ source_relation }} AS source
  CROSS JOIN {{ target_relation }} AS target
  WHERE
    {% if spatial_fn %}
      {{ spatial_fn }}(
        ST_GeomFromText(source.{{ source_column }}),
        ST_GeomFromText(target.{{ target_column }})
      )
    {% elif match_type == 'touches_or_intersects' %}
      ST_Touches(
        ST_GeomFromText(source.{{ source_column }}),
        ST_GeomFromText(target.{{ target_column }})
      )
      OR ST_Intersects(
        ST_GeomFromText(source.{{ source_column }}),
        ST_GeomFromText(target.{{ target_column }})
      )
    {% elif match_type == 'envelope' %}
      ST_Intersects(
        ST_Envelope(ST_GeomFromText(source.{{ source_column }})),
        ST_Envelope(ST_GeomFromText(target.{{ target_column }}))
      )
    {% else %}
      1=1 -- fallback if no known type
    {% endif %}

{%- endmacro %}
