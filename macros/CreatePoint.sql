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