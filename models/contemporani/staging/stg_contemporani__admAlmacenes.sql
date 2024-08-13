with 

source as (

    select * from {{ source('contemporani', 'admAlmacenes') }}

),

renamed as (

    select
        cidalmacen,
        ccodigoalmacen,
        cnombrealmacen
        -- cfechaaltaalmacen,
        -- cidvalorclasificacion1,
        -- cidvalorclasificacion2,
        -- cidvalorclasificacion3,
        -- cidvalorclasificacion4,
        -- cidvalorclasificacion5,
        -- cidvalorclasificacion6,
        -- csegcontalmacen,
        -- ctextoextra1,
        -- ctextoextra2,
        -- ctextoextra3,
        -- cfechaextra,
        -- cimporteextra1,
        -- cimporteextra2,
        -- cimporteextra3,
        -- cimporteextra4,
        -- cbandomicilio,
        -- ctimestamp,
        -- cscalmac2,
        -- cscalmac3,
        -- csistorig

    from source

)

select * from renamed
