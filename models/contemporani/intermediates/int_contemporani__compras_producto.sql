with

productos as (
    select 
        cidproducto,
        ccodigoproducto,
        cnombreproducto,
        ccontrolexistencia,
        cstatusproducto,
        cprecio1,
        subtipo,
        subsubtipo,
        proveedor,
        tipo_base_de_datos,
        tipo

    from {{ ref('int_contemporani_products_info') }}

    where 
        ctipoproducto = 1
        and ccodigoproducto = 'CI5786'
),

compras as (
    select
        cidproducto,
        cidproveedor,
        cpreciocompra,
        cidmoneda,
        fecha_compra
    from {{ ref('stg_contemporani__admPreciosCompra') }}
),

joins as (
    select 
        p.*,
        c.* except(cidproducto)

    from productos p
    join compras c
        on p.cidproducto = c.cidproducto
)

select * from joins