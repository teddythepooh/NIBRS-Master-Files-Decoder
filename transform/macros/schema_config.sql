{% macro generate_schema_name(custom_schema_name, node) -%}
{#
    This is taken from https://docs.getdbt.com/docs/build/custom-schemas. By default,
    dbt appends the target schema to a custom schema defined in dbt_project.yml. This macro overrides this,
    such that custom schema names are no longer prefixed with the target schema.
#}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name }}

    {%- endif -%}

{%- endmacro %}
