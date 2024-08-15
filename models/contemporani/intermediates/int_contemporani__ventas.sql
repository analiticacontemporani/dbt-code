with 

almacenes as (

    select * from {{ ref('stg_contemporani__admAlmacenes') }}

),

existencia_productos as (

    select
        cidproducto,
        cidalmacen,
        existencia
    from {{ ref('stg_contemporani__admExistenciaCosto') }}
    where existencia > 0

),

productos as (

    select * from {{ ref('int_contemporani_products_info') }}
    where cstatusproducto = 1

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
    ep.existencia,
    p.cprecio1,
    p.tipo_base_de_datos

from existencia_productos ep
join almacenes a 
    on a.cidalmacen = ep.cidalmacen
join productos p 
    on p.cidproducto = ep.cidproducto

order by a.cidalmacen asc, p.ccodigoproducto asc