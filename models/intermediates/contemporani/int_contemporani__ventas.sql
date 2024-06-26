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

    select * from {{ ref('stg_contemporani__admProductos') }}

),

clasificaciones_valores as (

    select * from {{ ref('stg_contemporani__admClasificacionesValores') }}

)

select
    a.cnombrealmacen,
    cv.cvalorclasificacion,
    p.ccodigoproducto,
    p.cnombreproducto,
    cp.existencia,
    p.cprecio1

from capas_producto cp
    left join almacenes a on a.cidalmacen = cp.cidalmacen
    left join productos p on p.cidproducto = cp.cidproducto
    left join clasificaciones_valores cv on cv.cidvalorclasificacion = p.cidvalorclasificacion1

order by a.cidalmacen asc, cv.cvalorclasificacion asc, p.ccodigoproducto asc