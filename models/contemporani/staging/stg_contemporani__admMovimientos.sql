with 

source as (

    select * from {{ source('contemporani', 'admMovimientos') }}

),

renamed as (

    select
        cidmovimiento,
        ciddocumento,
        cnumeromovimiento,
        ciddocumentode,
        cidproducto,
        cidalmacen,
        cunidades,
        cunidadesnc,
        cunidadescapturadas,
        cidunidad,
        cidunidadnc,
        cprecio,
        cpreciocapturado,
        ccostocapturado,
        ccostoespecifico,
        cneto,
        cimpuesto1,
        cporcentajeimpuesto1,
        cimpuesto2,
        cporcentajeimpuesto2,
        cimpuesto3,
        cporcentajeimpuesto3,
        cretencion1,
        cporcentajeretencion1,
        cretencion2,
        cporcentajeretencion2,
        cdescuento1,
        cporcentajedescuento1,
        cdescuento2,
        cporcentajedescuento2,
        cdescuento3,
        cporcentajedescuento3,
        cdescuento4,
        cporcentajedescuento4,
        cdescuento5,
        cporcentajedescuento5,
        ctotal,
        cporcentajecomision,
        creferencia,
        cobservamov,
        cafectaexistencia,
        cafectadosaldos,
        cafectadoinventario,
        cfecha,
        cmovtooculto,
        cidmovtoowner,
        cidmovtoorigen,
        cunidadespendientes,
        cunidadesncpendientes,
        cunidadesorigen,
        cunidadesncorigen,
        ctipotraspaso,
        cidvalorclasificacion,
        ctextoextra1,
        ctextoextra2,
        ctextoextra3,
        cfechaextra,
        cimporteextra1,
        cimporteextra2,
        cimporteextra3,
        cimporteextra4,
        ctimestamp,
        cgtomovto,
        cscmovto,
        ccomventa,
        cidmovtodestino,
        cnumeroconsolidaciones,
        cobjimpu01

    from source

)

select * from renamed
