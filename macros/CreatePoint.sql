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

{%- macro snowflake__CreatePoint(
        relation, matchFields
) -%}

    {%- set invalid_fields = [] -%}
    {%- for fields in matchFields %}
        {%- if fields[0] | length == 0
              or fields[1] | length == 0
              or fields[2] | length == 0 %}
            {%- do invalid_fields.append(true) %}
        {%- endif %}
    {%- endfor %}

    {%- set rel = prophecy_basics.get_relation(relation) -%}
    {%- if matchFields | length == 0 or invalid_fields | length > 0 %}
        SELECT *
        FROM {{ rel }}
    {%- else %}

        SELECT
            base.*,
            {%- for fields in matchFields %}
                CONCAT(
                    'POINT(',
                    base.{{ prophecy_basics.quote_identifier(fields[0]) }},
                    ' ',
                    base.{{ prophecy_basics.quote_identifier(fields[1]) }},
                    ')'
                ) AS {{ prophecy_basics.quote_identifier(fields[2]) }}
                {%- if not loop.last %},{% endif %}
            {%- endfor %}
        FROM {{ rel }} AS base
    {%- endif %}
{%- endmacro %}