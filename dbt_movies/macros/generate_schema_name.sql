{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
      If no custom schema is passed, use the target dataset (analytics_movies).
      If a custom schema is explicitly set (e.g. 'staging'), keep it clean without default prefixes.
    -#}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

