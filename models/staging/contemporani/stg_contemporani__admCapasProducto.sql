with 

source as (

    select * from {{ source('contemporani', 'admCapasProducto') }}

),

renamed as (

    select
        cidcapa,
        cidalmacen,
        cidproducto,
        cfecha,
        ctipocapa,
        cnumerolote,
        cfechacaducidad,
        cfechafabricacion,
        cpedimento,
        caduana,
        cfechapedimento,
        ctipocambio,
        cexistencia,
        ccosto,
        cidcapaorigen,
        ctimestamp,
        cnumaduana,
        cclavesat

    from source

)

select * from renamed
