with 

movimientos_compras as (
    select * from {{ ref('int_contemporani__compras_movimientos') }}
),

productos as (
    select * from {{ ref('int_contemporani_products_info') }}
),

documentos as (
    select * from {{ ref('int_contemporani__compras_documentos') }}
)

select
    doc.ciddocumento,
    doc.cidconceptodocumento,
    doc.cfecha,
    doc.crazonsocial,
    mov.cidmovimiento,
    mov.cunidades,
    mov.ccostoespecifico,
    mov.cneto,
    prod.ccodigoproducto,
    prod.cnombreproducto,
    prod.ctipoproducto, 
    prod.cprecio1,
    prod.tipo,
    prod.subtipo,
    prod.subsubtipo,
    prod.proveedor

from documentos doc
join movimientos_compras mov
    on doc.ciddocumento = mov.ciddocumento
join productos prod
    on mov.cidproducto = prod.cidproducto
order by doc.cfecha desc, doc.ciddocumento, mov.cidmovimiento, prod.ccodigoproducto
