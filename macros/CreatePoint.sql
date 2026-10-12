{#
  CreatePoint Macro Gem
  =====================

  Builds WKT POINT strings from separate longitude and latitude columns and
  exposes them as new columns. If there is nothing to build (empty match list or
  incomplete triples), compilation fails with a dbt compiler error.

  Parameters:
    - relation (list): One-element list naming the source table (same role as
      relation_name elsewhere), e.g. ['locations']; default__ wraps that name in
      backticks.
    - matchFields (list of triples): Each entry is [lon_col, lat_col, out_col].
      If any of the three names is empty, or matchFields is empty, the macro
      raises a compiler error naming the missing field.

  Adapter Support:
    - Default (backtick-quoted table/columns; CONCAT('POINT (', lon, ' ', lat, ')') per triple)

  Depends on schema parameter:
    No

  Macro Call Examples:
    {{ prophecy_spatial.CreatePoint(['locations'], [['longitude', 'latitude', 'geom_point']]) }}

  CTE Usage Example:
    Macro call (example above):
      {{ prophecy_spatial.CreatePoint(['locations'], [['longitude', 'latitude', 'geom_point']]) }}

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
    {%- if matchFields | length == 0 %}
        {{ exceptions.raise_compiler_error("CreatePoint: Please add at least one point.") }}
    {%- endif %}
    {%- for fields in matchFields %}
        {%- if (fields[0] or '') | trim == '' %}
            {{ exceptions.raise_compiler_error("CreatePoint: Please select the longitude column name.") }}
        {%- endif %}
        {%- if (fields[1] or '') | trim == '' %}
            {{ exceptions.raise_compiler_error("CreatePoint: Please select the latitude column name.") }}
        {%- endif %}
        {%- if (fields[2] or '') | trim == '' %}
            {{ exceptions.raise_compiler_error("CreatePoint: Please provide a target column name.") }}
        {%- endif %}
    {%- endfor %}

        select
            *,
            {%- for fields in matchFields %}
                CONCAT('POINT (', `{{ fields[0] }}`, ' ', `{{ fields[1] }}`, ')') as `{{ fields[2] }}`{% if not loop.last %},{% endif %}
            {%- endfor %}
        from `{{ relation }}`
{%- endmacro -%}