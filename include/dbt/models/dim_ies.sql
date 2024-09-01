SELECT
  CO_MANTENEDORA as idMantenedora,
  NO_MANTENEDORA as nomeMantenedora,
  CO_IES as idIES,
  SG_IES as siglaIes,
  NO_IES as nomeIes,
  QT_DOC_TOTAL as qtdTotalDocentes,
  QT_DOC_EXE as qtdTotalDocentesAtivos,
  QT_DOC_EX_FEMI as qtdDocentesFemininos,
  QT_DOC_EX_MASC as qtdDocentesMasculinos,
  CASE TP_ORGANIZACAO_ACADEMICA
    WHEN 1 THEN 'Universidade'
    WHEN 2 THEN 'Centro Universitário'
    WHEN 3 THEN 'Faculdade'
    WHEN 4 THEN 'Instituto Federal de Educação, Ciência e Tecnologia'
    WHEN 5 THEN 'Centro Federal de Educação Tecnológica'
    ELSE 'Unknown'
  END as organizacaoAcademica,
  CASE TP_CATEGORIA_ADMINISTRATIVA
    WHEN 1 THEN 'Pública Federal'
    WHEN 2 THEN 'Pública Estadual'
    WHEN 3 THEN 'Pública Municipal'
    WHEN 4 THEN 'Privada com fins lucrativos'
    WHEN 5 THEN 'Privada sem fins lucrativos'
    WHEN 6 THEN 'Privada - Particular em sentido estrito'
    WHEN 7 THEN 'Especial'
    WHEN 8 THEN 'Privada comunitária'
    WHEN 9 THEN 'Privada confessional'
    ELSE 'Unknown'
  END AS categoriaAdministrativa
FROM {{ source('censo_superior', 'stg_ies') }}