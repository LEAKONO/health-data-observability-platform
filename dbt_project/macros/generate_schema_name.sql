{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        By default dbt prefixes custom schemas with the target schema
        (e.g. STAGING_MARTS). We override this so models land directly
        in the schema they declare in dbt_project.yml (RAW, STAGING,
        MARTS, OBSERVABILITY), matching the schemas setup.sql created.
    -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}