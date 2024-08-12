with 

compras as (
    select
        * except(fecha_cambio, cimporte)

    from {{ ref('int_contemporani__compras_producto') }}
    order by fecha_compra desc
)

select * from compras