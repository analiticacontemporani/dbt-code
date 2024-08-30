with

final as (

    select * 
    from {{ ref('int_contemporani__ventas') }}
    where 
        ccodigoalmacen != '2' --  No deberia de haber productos en el almacen 2, son errores de contpaq
        and ccodigoproducto not IN ('PTOTAL', 'FLETE') -- No son productos verdaderos, son productos que ayudan para la operacion


)

select * from final
