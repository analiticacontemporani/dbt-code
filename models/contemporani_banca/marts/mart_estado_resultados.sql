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
        
),

asociaciones as (

    select
        id,
        idctasup,
        idsubctade
    
    from {{ ref('stg_contemporani_banca__Asociaciones') }}

),

cuentas as (

    select 
        id,
        nombre,
        ctamayor

    from {{ ref('stg_contemporani_banca__Cuentas') }}

),

cuenta_mayor_join_superior as (

    select
        c_superior.nombre as cuenta_superior,
        c_mayor.id as id_mayor,
        c_mayor.nombre as cuenta_mayor,
        s_mayor.fecha_periodo,

        case
            when s_mayor.tipo = 'Resultados Deudora' then (s_mayor.cargos - IFNULL(s_mayor.abonos, 0)) * -1
            when s_mayor.tipo = 'Resultados Acreedora' then s_mayor.abonos - IFNULL(s_mayor.cargos, 0)
        end as importe_mayor

    from cuentas c_mayor
    join saldos_cuentas s_mayor
        on c_mayor.id = s_mayor.id and c_mayor.ctamayor = 1
    join asociaciones a_superior
        on c_mayor.id = a_superior.idsubctade
    join cuentas c_superior
        on a_superior.idctasup = c_superior.id

),

cuenta_mayor_join_inferior as (
    select 
        c_mayor.*,
        s_menor.nombre as cuenta_menor,

        case
            when s_menor.tipo = 'Resultados Deudora' then (s_menor.cargos - IFNULL(s_menor.abonos, 0)) * -1
            when s_menor.tipo = 'Resultados Acreedora' then s_menor.abonos - IFNULL(s_menor.cargos, 0)
        end as importe_menor

    from cuenta_mayor_join_superior c_mayor
    left join asociaciones a_menor
        on c_mayor.id_mayor = a_menor.idctasup
    left join saldos_cuentas s_menor
         on a_menor.idsubctade = s_menor.id and c_mayor.fecha_periodo = s_menor.fecha_periodo
),

joins as (
    select
        cuenta_superior,
        id_mayor, 
        cuenta_mayor,
        fecha_periodo, 
        importe_mayor, 
        cuenta_menor, 
        importe_menor,
        case
            when cuenta_menor is null then importe_mayor
            when cuenta_menor is not null then importe_menor
        end as importe

    from cuenta_mayor_join_inferior
)

select *
from joins