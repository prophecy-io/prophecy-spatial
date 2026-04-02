{#
  CreatePoint Macro Gem
  =====================

  Builds WKT POINT strings from separate longitude and latitude columns and
  exposes them as new columns. If there is nothing to build (empty match list or
  incomplete triples), returns all columns from the relation unchanged.

  Parameters:
    - relation (string): Source table name; default__ wraps it in backticks.
    - matchFields (list of triples): Each entry is [lon_col, lat_col, out_col].
      If any of the three names is empty-length, or matchFields is empty, the
      macro emits SELECT * FROM the relation only.

  Adapter Support:
    - Default (backtick-quoted table/columns; CONCAT('POINT (', lon, ' ', lat, ')') per triple)

  Depends on schema parameter:
    No

  Macro Call Examples:
    {{ prophecy_spatial.CreatePoint('locations', [['longitude', 'latitude', 'geom_point']]) }}

  CTE Usage Example:
    Macro call (example above):
      {{ prophecy_spatial.CreatePoint('locations', [['longitude', 'latitude', 'geom_point']]) }}

    Resolved query (default__):
      SELECT
          *,
          CONCAT('POINT (', `longitude`, ' ', `latitude`, ')') as `geom_point`
      from `locations`
#}
{% macro CreatePoint(relation, matchFields) -%}
    {{ return(adapter.dispatch('CreatePoint', 'prophecy_spatial')(relation, matchFields)) }}
{% endmacro %}


{%- macro default__CreatePoint(
        relation, matchFields
) -%}
    {%- set invalid_fields = [] -%}
    {%- for fields in matchFields %}
        {%- if fields[0] | length == 0 or fields[1] | length == 0 or fields[2] | length == 0 %}
            {%- do invalid_fields.append(true) %}
        {%- endif %}
    {%- endfor %}

    {%- if matchFields | length == 0 or invalid_fields | length > 0 %}
        select * from `{{ relation }}`
    {%- else %}
        select
            *,
            {%- for fields in matchFields %}
                CONCAT('POINT (', `{{ fields[0] }}`, ' ', `{{ fields[1] }}`, ')') as `{{ fields[2] }}`{% if not loop.last %},{% endif %}
            {%- endfor %}
        from `{{ relation }}`
    {%- endif %}
{%- endmacro -%}