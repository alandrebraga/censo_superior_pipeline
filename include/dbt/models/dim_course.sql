SELECT DISTINCT
    CO_CURSO as idCurso,
    NO_CURSO as nomeCurso,
    CO_CINE_ROTULO as idCineRotulo,
    NO_CINE_ROTULO as cineRotulo,
    NO_CINE_AREA_GERAL as areaGeral,
    CO_CINE_AREA_ESPECIFICA as areaEspecifica
FROM {{ source('censo_superior', 'stg_cursos') }}