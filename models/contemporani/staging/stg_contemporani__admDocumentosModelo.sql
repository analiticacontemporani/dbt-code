with 

source as (

    select * from {{ source('contemporani', 'admDocumentosModelo') }}

),

renamed as (

    select
        ciddocumentode,
        cdescripcion,
        cnaturaleza,
        cafectaexistencia,
        cmodulo,
        cnofolio,
        cidconceptodoctoasumido,
        cusacliente,
        cusaproveedor,
        cidasientocontable

    from source

)

select * from renamed
