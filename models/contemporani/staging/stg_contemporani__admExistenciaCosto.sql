with 

source as (

    select * from {{ source('contemporani', 'admExistenciaCosto') }}

),

renamed as (

    select
        cidexistencia,
        cidalmacen,
        cidproducto,
        cidejercicio,
        ctipoexistencia,
        centradasiniciales - csalidasiniciales as existencia
    from source

),

current_year as (
    select * 
    from renamed 
    where 
        cidejercicio = (select max(cidejercicio) from renamed)
)

select * from current_year
