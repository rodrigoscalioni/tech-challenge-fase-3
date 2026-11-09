# Tech Challenge — Fase 4 | Amazon Dataprep

Pipeline Python de preparação da base Kaggle *Amazon Sales Dataset* para um dashboard de **Power BI**. Código desenvolvido independentemente, usando as consultas do projeto do grupo como referência de requisitos.

## Execução

Requisitos: Python 3.10+ e `pandas>=2.0,<3.0`. Na raiz do repositório:

```bash
python -m pip install -r fase4_amazon/requirements.txt
python fase4_amazon/src/dataprep.py --input /caminho/para/amazon.csv --output-dir fase4_amazon/data/processed
python -m unittest discover -s fase4_amazon/tests -v
```

Substitua `/caminho/para/amazon.csv` pelo caminho do arquivo fornecido na disciplina. A base original **não** precisa ser publicada no GitHub.

## Arquivos gerados

- **products.csv**: uma linha por `product_id`. Preços em INR, desconto em escala de 0 a 1, nota, quantidade de avaliações, categorias hierárquicas e alertas de conflito. Use como dimensão/fato de indicadores de produtos.
- **review_bundles.csv**: conjuntos de comentários, vinculados por `product_id`. Contém os textos originais e tratados, sem presumir que cada linha é uma avaliação individual.
- **quality_report.json**: contagens, ausências, inconsistências e hipóteses adotadas.

Power BI: relacione `products[product_id]` (lado **1**) a `review_bundles[product_id]` (lado **muitos**), filtro em direção única de produtos para comentários. Use `products` para KPIs de produtos: misturar com a tabela de comentários pode duplicar contagens e valores. Formate `discount_pct` como percentual. Valores monetários estão em **₹ (INR)**, não em R$.

## Decisões de qualidade e limitações

1. Produtos duplicados são consolidados pelo `product_id`. O registro escolhido tem maior `rating_count` válido, seguido de completude e ordem de origem. Não é uma deduplicação temporal, pois não há datas.
2. `rating_count` é um **estoque de avaliações atribuídas ao produto**, não volume de vendas, visualizações ou comentários coletados neste CSV; **não somamos** valores em duplicatas.
3. `rating`, preços e descontos inválidos tornam-se ausentes (`NaN`), e são auditados. Não removemos produtos por uma única informação faltante.
4. Textos do Kaggle frequentemente combinam vários reviews em uma única célula separados por vírgula. Como os comentários também contêm vírgulas, **não separamos** esse conteúdo em supostas avaliações individuais. Analise temas qualitativamente nos *bundles*, sem chamar seu número de "total de avaliações".
5. Sem data de captura, vendas, receita ou visualizações, não se inferem crescimento, faturamento, causalidade de descontos ou visibilidade real. `rating_count` é apenas um proxy de engajamento.
6. Conservamos a moeda original (INR), sem conversão online que comprometeria a reprodutibilidade.
7. O relatório registra conflitos entre observações de um mesmo produto, inclusive preços, descontos e `rating_count`.

## Próxima etapa

Analisar distribuição de ratings, categorias, faixas de desconto, padrões de engajamento e temas textuais; construir páginas do Power BI e documentar recomendações com evidências. Este pacote entrega **somente a preparação**.
