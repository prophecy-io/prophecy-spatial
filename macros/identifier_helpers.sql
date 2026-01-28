{# Helper macros for proper identifier handling across different database engines #}

{% macro quote_identifier(identifier) %}
    {{ return(adapter.dispatch('quote_identifier', 'prophecy_spatial')(identifier)) }}
{% endmacro %}

{% macro default__quote_identifier(identifier) %}
    {% if identifier is string %}
        {% set clean_identifier = identifier | trim %}
        {% if ' ' in clean_identifier or clean_identifier.startswith('`') == false %}
            {% set bt = "`" %}
            {{ bt ~ clean_identifier ~ bt }}
        {% else %}
            {{ clean_identifier }}
        {% endif %}
    {% else %}
        {{ identifier }}
    {% endif %}
{% endmacro %}

{% macro duckdb__quote_identifier(identifier, quote_char='"') %}
    {%- if identifier is string -%}
        {# Single identifier - return quoted string #}
        {{ quote_char }}{{ identifier }}{{ quote_char }}
    {%- elif identifier is iterable -%}
        {# List of identifiers - return list of quoted strings #}
        {%- set quoted_identifiers = [] -%}
        {%- for id in identifier -%}
            {%- do quoted_identifiers.append(quote_char ~ id ~ quote_char) -%}
        {%- endfor -%}
        {{ quoted_identifiers }}
    {%- else -%}
        {# Fallback: treat as string #}
        {{ quote_char }}{{ identifier }}{{ quote_char }}
    {%- endif -%}
{% endmacro %}

{% macro unquote_identifier(identifier) %}
    {{ return(adapter.dispatch('unquote_identifier', 'prophecy_spatial')(identifier)) }}
{% endmacro %}

{% macro default__unquote_identifier(identifier) %}
    {% if identifier is string %}
        {% set clean_identifier = identifier | trim %}
        {% if clean_identifier.startswith('`') and clean_identifier.endswith('`') %}
            {{ clean_identifier[1:-1] }}
        {% else %}
            {{ clean_identifier }}
        {% endif %}
    {% else %}
        {{ identifier }}
    {% endif %}
{% endmacro %}

{% macro duckdb__unquote_identifier(identifier) %}
    {%- if identifier is string -%}
        {# Single identifier - unquote if quoted (handles both double quotes and backticks) #}
        {%- set clean_identifier = identifier | trim -%}
        {%- if clean_identifier.startswith('"') and clean_identifier.endswith('"') -%}
            {{ clean_identifier[1:-1] }}
        {%- elif clean_identifier.startswith('`') and clean_identifier.endswith('`') -%}
            {{ clean_identifier[1:-1] }}
        {%- else -%}
            {{ clean_identifier }}
        {%- endif -%}
    {%- elif identifier is iterable -%}
        {# List of identifiers - return list of unquoted strings #}
        {%- set unquoted_identifiers = [] -%}
        {%- for id in identifier -%}
            {%- set clean_id = id | trim -%}
            {%- if clean_id.startswith('"') and clean_id.endswith('"') -%}
                {%- do unquoted_identifiers.append(clean_id[1:-1]) -%}
            {%- elif clean_id.startswith('`') and clean_id.endswith('`') -%}
                {%- do unquoted_identifiers.append(clean_id[1:-1]) -%}
            {%- else -%}
                {%- do unquoted_identifiers.append(clean_id) -%}
            {%- endif -%}
        {%- endfor -%}
        {{ unquoted_identifiers }}
    {%- else -%}
        {# Fallback: treat as string #}
        {{ identifier }}
    {%- endif -%}
{% endmacro %}

{% macro safe_identifier(identifier) %}
    {{ return(adapter.dispatch('safe_identifier', 'prophecy_spatial')(identifier)) }}
{% endmacro %}

{% macro default__safe_identifier(identifier) %}
    {# Returns a safe identifier that can be used in SQL queries #}
    {% if identifier is string %}
        {% set clean_identifier = identifier | trim %}
        {% if ' ' in clean_identifier or clean_identifier.startswith('`') == false %}
            {% set bt = "`" %}
            {{ bt ~ clean_identifier ~ bt }}
        {% else %}
            {{ clean_identifier }}
        {% endif %}
    {% else %}
        {{ identifier }}
    {% endif %}
{% endmacro %}

{% macro duckdb__safe_identifier(identifier, quote_char='"') %}
    {# Returns a safe identifier that can be used in SQL queries #}
    {%- if identifier is string -%}
        {# Single identifier - return quoted string #}
        {{ quote_char }}{{ identifier }}{{ quote_char }}
    {%- elif identifier is iterable -%}
        {# List of identifiers - return list of quoted strings #}
        {%- set quoted_identifiers = [] -%}
        {%- for id in identifier -%}
            {%- do quoted_identifiers.append(quote_char ~ id ~ quote_char) -%}
        {%- endfor -%}
        {{ quoted_identifiers }}
    {%- else -%}
        {# Fallback: treat as string #}
        {{ quote_char }}{{ identifier }}{{ quote_char }}
    {%- endif -%}
{% endmacro %}

{% macro quote_column_list(column_list) %}
    {# Takes a comma-separated string of column names and returns a quoted, comma-separated string #}
    {{ return(adapter.dispatch('quote_column_list', 'prophecy_spatial')(column_list)) }}
{% endmacro %}

{% macro default__quote_column_list(column_list) %}
    {%- if column_list == "" or column_list is none -%}
        {{ return("") }}
    {%- else -%}
        {%- set quoted_columns = [] -%}
        {%- set column_list_split = column_list.split(',') | map('trim') | list -%}
        {%- for col in column_list_split -%}
            {%- if col != "" -%}
                {%- do quoted_columns.append(prophecy_spatial.quote_identifier(col)) -%}
            {%- endif -%}
        {%- endfor -%}
        {{ return(quoted_columns | join(', ')) }}
    {%- endif -%}
{% endmacro %}
