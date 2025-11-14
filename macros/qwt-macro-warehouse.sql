{% macro grant_warehouse(warehouse)%}

{%set querywh%}

alter warehouse {{target.warehouse}} set warehouse_size = 'medium'
{% endset %}

{% do run_query(querywh)%}
{% do log("access is given", info=true)%}

{% endmacro %}