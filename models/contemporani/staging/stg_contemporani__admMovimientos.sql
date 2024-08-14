with 

source as (

    select * from {{ source('contemporani', 'admMovimientos') }}

),

renamed as (

    select
        cidmovimiento,
        ciddocumento,
        ciddocumentode,
        cidproducto,
        cidalmacen,
        cunidades,
        cunidadescapturadas,
        ccostoespecifico,
        cneto,
        ctotal,
        cdescuento1

        -- cnumeromovimiento,
        -- cunidadesnc,
        -- cidunidad,
        -- cidunidadnc,
        -- cprecio,
        -- cpreciocapturado,
        -- ccostocapturado,
        -- cimpuesto1,
        -- cporcentajeimpuesto1,
        -- cimpuesto2,
        -- cporcentajeimpuesto2,
        -- cimpuesto3,
        -- cporcentajeimpuesto3,
        -- cretencion1,
        -- cporcentajeretencion1,
        -- cretencion2,
        -- cporcentajeretencion2,
        -- cporcentajedescuento1,
        -- cdescuento2,
        -- cporcentajedescuento2,
        -- cdescuento3,
        -- cporcentajedescuento3,
        -- cdescuento4,
        -- cporcentajedescuento4,
        -- cdescuento5,
        -- cporcentajedescuento5,
        -- cporcentajecomision,
        -- creferencia,
        -- cobservamov,
        -- cafectaexistencia,
        -- cafectadosaldos,
        -- cafectadoinventario,
        -- cfecha,
        -- cmovtooculto,
        -- cidmovtoowner,
        -- cidmovtoorigen,
        -- cunidadespendientes,
        -- cunidadesncpendientes,
        -- cunidadesorigen,
        -- cunidadesncorigen,
        -- ctipotraspaso,
        -- cidvalorclasificacion,
        -- ctextoextra1,
        -- ctextoextra2,
        -- ctextoextra3,
        -- cfechaextra,
        -- cimporteextra1,
        -- cimporteextra2,
        -- cimporteextra3,
        -- cimporteextra4,
        -- ctimestamp,
        -- cgtomovto,
        -- cscmovto,
        -- ccomventa,
        -- cidmovtodestino,
        -- cnumeroconsolidaciones,
        -- cobjimpu01

    from source

)

select * from renamed
