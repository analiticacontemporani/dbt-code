with

saldos_cuentas as (

    select 
        id,
        nombre,
        fecha_periodo,
        tipo,
        cuenta_mayor,
        cargos,
        abonos

    from {{ ref('int_contemporani_banca__saldos_cuentas') }}

    where 
        tipo in ('Resultados Deudora', 'Resultados Acreedora')
        and (cuenta_mayor = 'Mayor'  or id in(889, 890)) -- Deberia ser cuenta mayor
),

asociaciones as (

    select
        id,
        idctasup,
        idSubCtade
    
    from {{ ref('stg_contemporani_banca__Asociaciones') }}

),

cuentas as (

    select 
        id,
        nombre

    from {{ ref('stg_contemporani_banca__Cuentas') }}

),


joins as (

    select 
        c.nombre as categoria_superior,
        sc.* EXCEPT(cargos, abonos),

        case
            when tipo = 'Resultados Deudora' then (cargos - IFNULL(abonos, 0)) * -1
            when tipo = 'Resultados Acreedora' then abonos - IFNULL(cargos, 0)
            else -1
        end as importe

    from saldos_cuentas sc
    join asociaciones a
        on sc.id = a.idSubCtade
    join cuentas c
        on a.idctasup = c.id

)

select * from joins
order by fecha_periodo, categoria_superior, nombre