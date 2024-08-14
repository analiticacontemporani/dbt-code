with 

documentos as (
    select * from {{ ref('int_contemporani__compras_documentos') }}
)

select * from documentos
