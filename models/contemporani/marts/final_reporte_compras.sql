with 

compras as (
    select
        cidproducto,
        ccodigoproducto,
        cnombreproducto,
        proveedor,
        tipo,
        cidautoincsql,
        cpreciocompra,
        cidmoneda,
        fecha_compra,
        monto_compra_pesos

    from {{ ref('int_contemporani__compras_producto') }}
    order by fecha_compra desc
)

select * from compras