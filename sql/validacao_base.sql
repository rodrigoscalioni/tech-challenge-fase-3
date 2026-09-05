-- Tech Challenge - Fase 3
-- Validacao da camada principal de consumo

SELECT DISTINCT
    respondente_id,
    CAST(idade AS INTEGER) AS idade,
    genero,
    estado,
    uf,
    COALESCE(NULLIF(TRIM(regiao), ''), 'Não informado') AS regiao,
    cargo_atual,
    nivel,
    faixa_salarial,
    tempo_experiencia_dados,
    modelo_trabalho_atual,
    tipo_uso_ia_empresa,
    uso_ia_generativa,
    CAST(ano_pesquisa AS INTEGER) AS ano_pesquisa
FROM tech_challenge.pesquisa_data_hackers;

-- Resultado esperado após a deduplicação: 14.002 linhas/respondentes.
