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

cuentas_mayores as (
    select 
        id as id_mayor,
        nombre as cuenta_mayor

    from cuentas

    where
        ctamayor = 1
),

joins as (

    select 
        c_superior.nombre as categoria_superior,
        c_mayor.id_mayor,
        c_mayor.cuenta_mayor,
        c_menor.nombre as categoria_menor,
        sc_mayor.fecha_periodo,

        case
            when c_menor.id is not null then
                case
                    when sc_menor.tipo = 'Resultados Deudora' then (sc_menor.cargos - IFNULL(sc_menor.abonos, 0)) * -1
                    when sc_menor.tipo = 'Resultados Acreedora' then sc_menor.abonos - IFNULL(sc_menor.cargos, 0)
                end
            else
                case
                    when sc_mayor.tipo = 'Resultados Deudora' then (sc_mayor.cargos - IFNULL(sc_mayor.abonos, 0)) * -1
                    when sc_mayor.tipo = 'Resultados Acreedora' then sc_mayor.abonos - IFNULL(sc_mayor.cargos, 0)
                end
        end as importe

    from cuentas_mayores c_mayor
    join asociaciones a_superior
        on c_mayor.id_mayor = a_superior.idsubctade
    join cuentas c_superior
        on a_superior.idctasup = c_superior.id
    left join asociaciones a_menor
        on c_mayor.id_mayor = a_menor.idctasup
    left join cuentas c_menor
        on a_menor.idsubctade = c_menor.id
    left join saldos_cuentas sc_menor
        on sc_menor.id = c_menor.id
    left join saldos_cuentas sc_mayor
        on sc_mayor.id = c_mayor.id_mayor

    where
        sc_mayor.fecha_periodo is not null
)

select * 
from joins
order by 
    fecha_periodo desc, 
    categoria_superior, 
    cuenta_mayor, 
    categoria_menor