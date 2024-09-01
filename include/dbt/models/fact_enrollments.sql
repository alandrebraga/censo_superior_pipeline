SELECT
  CO_MUNICIPIO as idMunicipio,
  CO_IES as idIes,
  CO_CURSO as idCurso,
  QT_VG_TOTAL as qtdVagaTotal,
  QT_VG_NOVA as qtdVagaNova,
  QT_INSCRITO_TOTAL as qtdInscritosTotal,
  QT_INSC_VG_NOVA as qtdInscritoVagaNova,
  QT_ING_FEM as qtdIngressanteFeminino,
  QT_ING_MASC as qtdIngressanteMasculino,
  QT_MAT as qtdMatriculado,
  QT_MAT_FEM as qtdMatriculadoFeminino,
  QT_MAT_MASC as qtdMatriculadoMasculino,
  CASE TP_GRAU_ACADEMICO
    WHEN 1 THEN 'Bacharelado'
    WHEN 2 THEN 'Licenciatura'
    WHEN 3 THEN 'Tecnológico'
    WHEN 4 THEN 'Bacharelado e Licenciatura'
    ELSE 'Não aplicável'
  END AS grauAcademico,
  CASE TP_MODALIDADE_ENSINO
    WHEN 1 THEN 'Presencial'
    WHEN 2 THEN 'EAD'
  ELSE 'Não aplicável'
  END as modalidadeEnsino,
  CASE TP_REDE
    WHEN 1 THEN 'Pública'
    WHEN 2 THEN 'Privada'
  ELSE 'Não aplicável'
  END as redeEnsino
FROM {{ source('censo_superior', 'stg_cursos') }}