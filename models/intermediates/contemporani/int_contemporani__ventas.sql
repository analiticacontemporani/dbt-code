with 

almacenes as (

    select * from {{ ref('stg_contemporani__admAlmacenes') }}

),

capas_producto as (

    select
        cidproducto,
        cidalmacen,
        sum(cexistencia) as existencia
    from {{ ref('stg_contemporani__admCapasProducto') }}
    where cexistencia > 0
    group by cidproducto, cidalmacen

),

productos as (

    select * from {{ ref('int_contemporani_products_info') }}

)

select
    a.cnombrealmacen,
    a.ccodigoalmacen,
    p.tipo,
    p.subtipo,
    p.subsubtipo,
    p.proveedor,
    p.ccodigoproducto,
    p.cnombreproducto,
    cp.existencia,
    p.cprecio1

from capas_producto cp
    left join almacenes a on a.cidalmacen = cp.cidalmacen
    left join productos p on p.cidproducto = cp.cidproducto

order by a.cidalmacen asc, p.ccodigoproducto asc