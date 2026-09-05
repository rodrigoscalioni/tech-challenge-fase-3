# Pipeline de Dados

Este documento complementa o README com uma descrição técnica resumida da implementação.

## Fluxo

1. Os CSVs das pesquisas de 2023, 2024 e 2025 são armazenados no Amazon S3, na camada Bronze.
2. AWS Glue Crawlers identificam os schemas e registram as tabelas no database `tech_challenge`.
3. O AWS Glue Studio padroniza os campos de cada edição e realiza a união das três bases.
4. A saída é armazenada em Parquet com compressão Snappy na camada conceitual Gold. O caminho físico utilizado no projeto é `s3://dados-fase3/gold/pesquisa_data_hackers/`.
5. A tabela consolidada é registrada no Glue Data Catalog como `pesquisa_data_hackers`.
6. O job PySpark `tech_challenge_normalizacao_ia` deduplica a base e normaliza as duas perguntas multivaloradas de IA.
7. Os resultados são gravados em `gold/analytics/uso_ia_generativa/` e `gold/analytics/uso_ia_empresa/`.
8. O crawler `crawler-gold-analytics-ia` cataloga as duas estruturas separadamente.
9. O Amazon Athena valida os resultados e enriquece cada tabela de IA com as dimensões da base principal.
10. As bases de consumo são exportadas para utilização no Tableau Public.

## Controle de qualidade

A consolidação inicial contém 14.005 linhas. A deduplicação resulta em 14.002 respondentes únicos.

As estruturas multivaloradas são tratadas como associações entre respondente e categoria:

- `uso_ia_generativa`: 11.159 associações;
- `uso_ia_empresa`: 16.120 associações.

A cobertura de classificação das respostas não vazias foi de 100% nas duas variáveis.

## Decisão de modelagem das tabelas de IA

As duas estruturas normalizadas não são combinadas em uma única tabela de consumo. Como ambas possuem cardinalidade de um-para-muitos em relação ao respondente, juntá-las simultaneamente poderia multiplicar as linhas entre as categorias das duas perguntas.

Por isso, cada estrutura é enriquecida separadamente com atributos como ano, região, cargo, senioridade, experiência e modelo de trabalho.

## Observação metodológica

As pesquisas anuais não acompanham necessariamente os mesmos indivíduos. Comparações temporais devem ser interpretadas como mudanças na distribuição das respostas das amostras de cada ano, e não como evolução longitudinal dos mesmos profissionais.
