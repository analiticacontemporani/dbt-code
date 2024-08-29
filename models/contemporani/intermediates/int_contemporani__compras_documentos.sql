-- Obtener los documentos solamente de compras
with

documentos_compras as (
    select * 
    from {{ ref('stg_contemporani__admDocumentos') }}
    where ciddocumentode = 19  -- Documentos de Compras
        AND ccancelado = 0 -- Documentos no cancelados
)

select
    ciddocumento,
    ciddocumentode,
    cidconceptodocumento,
    cfecha,
    crazonsocial,
    ctipocambio,
    cgasto1 as importacion,
    cgasto2 as agente_aduanal,
    cgasto3 as flete,
    ctotalunidades,
    cneto,
    cneto * ctipocambio as cneto_pesos
    
from documentos_compras
order by cfecha desc