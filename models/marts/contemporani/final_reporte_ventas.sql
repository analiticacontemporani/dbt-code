with

final as (

    select
        cnombrealmacen as Almacen,
        cvalorclasificacion as Categoria,
        ccodigoproducto as Codigo,
        cnombreproducto as Producto,
        existencia as Existencia,
        cprecio1 as Precio

    from {{ ref('int_contemporani__ventas') }}

)

select *

from final

order by Almacen asc, Categoria asc, Producto asc