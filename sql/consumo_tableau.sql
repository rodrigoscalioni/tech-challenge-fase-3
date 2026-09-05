-- Tech Challenge - Fase 3
-- Bases analíticas enriquecidas para consumo no Tableau

-- ============================================================
-- 1. USO DE IA NAS EMPRESAS
-- ============================================================
SELECT DISTINCT
    p.respondente_id,
    CAST(p.ano_pesquisa AS INTEGER) AS ano_pesquisa,
    p.idade,
    p.genero,
    p.estado,
    p.uf,
    COALESCE(NULLIF(TRIM(p.regiao), ''), 'Não informado') AS regiao,
    p.cargo_atual,
    p.nivel,
    p.faixa_salarial,
    p.tempo_experiencia_dados,
    p.modelo_trabalho_atual,
    e.categoria_uso_ia_empresa
FROM tech_challenge.pesquisa_data_hackers p
INNER JOIN tech_challenge.uso_ia_empresa e
    ON p.respondente_id = e.respondente_id
   AND p.ano_pesquisa = e.ano_pesquisa
ORDER BY ano_pesquisa, respondente_id;

-- Resultado esperado: 16.120 associações.


-- ============================================================
-- 2. USO DE IA GENERATIVA
-- ============================================================
SELECT DISTINCT
    p.respondente_id,
    CAST(p.ano_pesquisa AS INTEGER) AS ano_pesquisa,
    p.idade,
    p.genero,
    p.estado,
    p.uf,
    COALESCE(NULLIF(TRIM(p.regiao), ''), 'Não informado') AS regiao,
    p.cargo_atual,
    p.nivel,
    p.faixa_salarial,
    p.tempo_experiencia_dados,
    p.modelo_trabalho_atual,
    g.categoria_uso_ia
FROM tech_challenge.pesquisa_data_hackers p
INNER JOIN tech_challenge.uso_ia_generativa g
    ON p.respondente_id = g.respondente_id
   AND p.ano_pesquisa = g.ano_pesquisa
ORDER BY ano_pesquisa, respondente_id;

-- Resultado esperado: 11.159 associações.

-- As duas estruturas são mantidas separadas propositalmente.
-- Um único JOIN entre base principal + ambas as tabelas multivaloradas
-- poderia gerar multiplicação cartesiana das categorias por respondente.
