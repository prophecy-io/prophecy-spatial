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

{%- macro duckdb__CreatePoint(
        relation, matchFields
) -%}
    {%- set invalid_fields = [] -%}
    {%- for fields in matchFields %}
        {%- if fields[0] | length == 0 or fields[1] | length == 0 or fields[2] | length == 0 %}
            {%- do invalid_fields.append(true) %}
        {%- endif %}
    {%- endfor %}

    {%- if matchFields | length == 0 or invalid_fields | length > 0 %}
        select * from {{ prophecy_spatial.quote_identifier(prophecy_spatial.unquote_identifier(relation)) }}
    {%- else %}
        select
            *,
            {%- for fields in matchFields %}
                ST_AsText(ST_Point({{ prophecy_spatial.quote_identifier(prophecy_spatial.unquote_identifier(fields[0])) }}, {{ prophecy_spatial.quote_identifier(prophecy_spatial.unquote_identifier(fields[1])) }})) as {{ prophecy_spatial.quote_identifier(prophecy_spatial.unquote_identifier(fields[2])) }}{% if not loop.last %},{% endif %}
            {%- endfor %}
        from {{ prophecy_spatial.quote_identifier(prophecy_spatial.unquote_identifier(relation)) }}
    {%- endif %}
{%- endmacro -%}

{%- macro bigquery__CreatePoint(
        relation, matchFields
) -%}
    {%- set invalid_fields = [] -%}
    {%- for fields in matchFields %}
        {%- if fields[0] | length == 0 or fields[1] | length == 0 or fields[2] | length == 0 %}
            {%- do invalid_fields.append(true) %}
        {%- endif %}
    {%- endfor %}

    {# sanitize relation name (Jinja-safe, no python methods) #}
    {% set _rel_raw = relation | string | replace('`', '') | replace("'", '') %}
    {% set _rel = _rel_raw | trim %}
    {% if '.' in _rel %}
        {% set rel_ident = '`' ~ (_rel | replace('.', '`.`')) ~ '`' %}
    {% else %}
        {% set rel_ident = '`' ~ _rel ~ '`' %}
    {% endif %}

    {%- if matchFields | length == 0 or invalid_fields | length > 0 %}
        select * from {{ rel_ident }}
    {%- else %}
        select
            *,
            {%- for fields in matchFields %}
                CONCAT('POINT (', `{{ fields[0] }}`, ' ', `{{ fields[1] }}`, ')') as `{{ fields[2] }}`{% if not loop.last %},{% endif %}
            {%- endfor %}
        from {{ rel_ident }}
    {%- endif %}
{%- endmacro -%}