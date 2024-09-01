SELECT DISTINCT
  CO_MUNICIPIO as idMunicipio,
  NO_MUNICIPIO as nomeMunicipio,
  SG_UF as siglaUF,
  in_capital as flCapital
FROM {{ source('censo_superior', 'stg_cursos') }}