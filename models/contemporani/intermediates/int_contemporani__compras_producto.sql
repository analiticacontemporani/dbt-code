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
        tipo,
        ctipoproducto

    from {{ ref('int_contemporani_products_info') }}

),

compras as (
    select
        cidautoincsql,
        cidproducto,
        cidproveedor,
        cpreciocompra,
        cidmoneda,
        fecha_compra
    from {{ ref('stg_contemporani__admPreciosCompra') }}
),

cambio_monedas as (
    select *
    from {{ ref('stg_contemporani__admTiposCambio') }}
    order by fecha_cambio desc 
),

joins as (
    select 
        p.*,
        c.* except(cidproducto),
        cm.cimporte,
        cm.fecha_cambio,
        ROW_NUMBER() OVER (PARTITION BY c.cidautoincsql, c.cidmoneda ORDER BY cm.fecha_cambio desc) as rn

    from productos p
    join compras c
        on p.cidproducto = c.cidproducto
    left join cambio_monedas cm
        on c.cidmoneda = cm.cidmoneda
        and c.fecha_compra >= cm.fecha_cambio
),

precio_pesos as (
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
        cimporte,
        fecha_cambio,

        case
            when cidmoneda = 1 then cpreciocompra
            when cidmoneda in (2,3) then cpreciocompra * cimporte
            else 0
        end as monto_compra_pesos
    
    from joins
    where rn=1

)

select *
from precio_pesos