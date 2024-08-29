-- Obtener los movimientos pertenecientes a los documentos de compras
with

movimientos_compras as (
    select *
    from {{ ref('stg_contemporani__admMovimientos') }}
    where ciddocumentode = 19
)

select * from movimientos_compras