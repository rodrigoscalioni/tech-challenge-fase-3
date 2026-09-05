"""
Tech Challenge — Fase 3
Normalização das respostas de Inteligência Artificial com AWS Glue + PySpark.

Objetivo
--------
Ler a base consolidada no Glue Data Catalog, remover duplicidades e transformar
duas perguntas multivaloradas em tabelas analíticas no formato
"respondente + categoria".

Entradas
--------
Glue Data Catalog:
    database: tech_challenge
    table:    pesquisa_data_hackers

Saídas
------
S3:
    s3://dados-fase3/gold/analytics/uso_ia_generativa/
    s3://dados-fase3/gold/analytics/uso_ia_empresa/

Observação
----------
A lógica de classificação abaixo depende de trechos textuais presentes nas
respostas originais da pesquisa. Por isso, as expressões de busca são mantidas
específicas, evitando classificações ambíguas.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ============================================================
# 1. INICIALIZAÇÃO DO JOB AWS GLUE
# ============================================================

# O nome do job é recebido automaticamente pelo AWS Glue em tempo de execução.
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Cria o contexto Spark e o contexto específico do AWS Glue.
sc = SparkContext()
glueContext = GlueContext(sc)

# Inicializa o controle de execução do Glue Job.
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ============================================================
# 2. LEITURA E DEDUPLICAÇÃO DA BASE CONSOLIDADA
# ============================================================

# A base consolidada já está catalogada no Glue Data Catalog.
# O DynamicFrame é convertido para DataFrame para permitir o uso das
# transformações nativas do PySpark.
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="tech_challenge",
    table_name="pesquisa_data_hackers"
)

df = dynamic_frame.toDF()

# Remove linhas integralmente duplicadas antes de qualquer normalização.
# Essa etapa garante que uma duplicidade na origem não gere associações
# repetidas nas tabelas analíticas.
df_base = df.dropDuplicates()

print("=== CONTAGEM APÓS DEDUPLICAÇÃO ===")
print(df_base.count())


# ============================================================
# 3. NORMALIZAÇÃO DO USO INDIVIDUAL DE IA GENERATIVA
# ============================================================

print("=== NORMALIZAÇÃO USO IA GENERATIVA ===")

df_ia = (
    df_base

    # Strings vazias não representam uma resposta válida.
    # Elas são convertidas para NULL para facilitar as validações posteriores.
    .withColumn(
        "uso_ia_generativa_limpo",
        F.when(
            F.trim(F.col("uso_ia_generativa")) == "",
            F.lit(None)
        ).otherwise(F.col("uso_ia_generativa"))
    )

    # A pergunta permite múltiplas escolhas em uma mesma resposta textual.
    # Por isso, cada regra abaixo verifica se determinado trecho está presente
    # e adiciona a categoria correspondente a um ARRAY.
    .withColumn(
        "categorias_ia",
        F.array(

            # Uso exclusivamente de soluções gratuitas.
            F.when(
                F.col("uso_ia_generativa_limpo")
                 .contains("Utilizo apenas soluções gratuitas"),
                F.lit("Gratuita")
            ),

            # Soluções pagas diretamente pelo próprio respondente.
            F.when(
                F.col("uso_ia_generativa_limpo")
                 .contains("pago do meu próprio bolso"),
                F.lit("Paga pelo próprio usuário")
            ),

            # Soluções financiadas pela empresa onde o respondente trabalha.
            F.when(
                F.col("uso_ia_generativa_limpo")
                 .contains("empresa em que trabalho paga"),
                F.lit("Paga pela empresa")
            ),

            # Ferramentas de IA direcionadas ao desenvolvimento de software.
            # As duas expressões aparecem em versões diferentes das pesquisas.
            F.when(
                (
                    F.col("uso_ia_generativa_limpo")
                     .contains('soluções no estilo "Copilot"')
                )
                |
                (
                    F.col("uso_ia_generativa_limpo")
                     .contains("soluções de AI para código")
                ),
                F.lit("IA para código / Copilot")
            ),

            # Respondentes que declararam não utilizar IA generativa.
            F.when(
                F.col("uso_ia_generativa_limpo")
                 .contains("Não utilizo nenhum tipo"),
                F.lit("Não utiliza")
            )
        )
    )

    # F.when retorna NULL quando a condição não é atendida.
    # O filtro mantém no ARRAY somente as categorias efetivamente identificadas.
    .withColumn(
        "categorias_ia",
        F.filter(
            F.col("categorias_ia"),
            lambda x: x.isNotNull()
        )
    )
)


# ============================================================
# 4. EXPLODE: UMA LINHA POR RESPONDENTE + CATEGORIA
# ============================================================

# O explode transforma o ARRAY de categorias em uma estrutura analítica:
#
# respondente_id | ano_pesquisa | categoria_uso_ia
#
# Assim, uma resposta com duas categorias gera duas associações, preservando
# corretamente a natureza multivalorada da pergunta.
df_ia_normalizado = (
    df_ia
    .select(
        "respondente_id",
        "ano_pesquisa",
        F.explode("categorias_ia").alias("categoria_uso_ia")
    )
)


# ============================================================
# 5. VALIDAÇÃO DA NORMALIZAÇÃO DE IA GENERATIVA
# ============================================================

# Distribuição final por ano e categoria.
# countDistinct é utilizado porque o interesse aqui é validar respondentes,
# e não apenas o número bruto de linhas após o explode.
print("=== DISTRIBUIÇÃO POR ANO E CATEGORIA ===")

(
    df_ia_normalizado
    .groupBy(
        "ano_pesquisa",
        "categoria_uso_ia"
    )
    .agg(
        F.countDistinct("respondente_id")
         .alias("respondentes")
    )
    .orderBy(
        "ano_pesquisa",
        F.desc("respondentes")
    )
    .show(50, truncate=False)
)

# Verifica respostas preenchidas que não foram associadas a nenhuma categoria.
# O resultado esperado após a validação das regras é zero.
print("=== RESPOSTAS NÃO CLASSIFICADAS ===")

(
    df_ia
    .filter(F.col("uso_ia_generativa_limpo").isNotNull())
    .filter(F.size("categorias_ia") == 0)
    .groupBy(
        "ano_pesquisa",
        "uso_ia_generativa_limpo"
    )
    .count()
    .orderBy(F.desc("count"))
    .show(30, truncate=False)
)

# Compara o total de respostas preenchidas com o total de respostas que
# receberam pelo menos uma categoria. Essa validação foi usada para confirmar
# a cobertura integral da classificação.
print("=== COBERTURA DA NORMALIZAÇÃO ===")

df_ia.groupBy("ano_pesquisa").agg(
    F.sum(
        F.when(
            F.col("uso_ia_generativa_limpo").isNotNull(),
            1
        ).otherwise(0)
    ).alias("respondentes_com_resposta"),

    F.sum(
        F.when(
            F.size("categorias_ia") > 0,
            1
        ).otherwise(0)
    ).alias("respondentes_classificados")
).orderBy("ano_pesquisa").show()


# ============================================================
# 6. NORMALIZAÇÃO DO USO DE IA NAS EMPRESAS
# ============================================================

print("=== NORMALIZAÇÃO USO IA EMPRESA ===")

df_empresa = (
    df_base

    # Padroniza respostas vazias como NULL.
    .withColumn(
        "tipo_uso_ia_empresa_limpo",
        F.when(
            F.trim(F.col("tipo_uso_ia_empresa")) == "",
            F.lit(None)
        ).otherwise(F.col("tipo_uso_ia_empresa"))
    )

    # Assim como na pergunta anterior, uma única resposta pode conter
    # múltiplas alternativas. Cada regra identifica uma categoria específica
    # e a adiciona ao ARRAY.
    .withColumn(
        "categorias_ia_empresa",
        F.array(

            # Uso individual por profissionais ou equipes, sem coordenação
            # corporativa central.
            F.when(
                F.col("tipo_uso_ia_empresa_limpo")
                 .contains("de forma independente"),
                F.lit("Uso individual / descentralizado")
            ),

            # A expressão abaixo é propositalmente específica.
            # Usar apenas "direcionamento centralizado" poderia gerar falso
            # positivo em respostas que dizem "sem um direcionamento centralizado".
            F.when(
                F.col("tipo_uso_ia_empresa_limpo")
                 .contains("Existe um direcionamento centralizado"),
                F.lit("Direcionamento corporativo")
            ),

            # Uso de IA pelas equipes de desenvolvimento.
            F.when(
                F.col("tipo_uso_ia_empresa_limpo")
                 .contains("Equipes de desenvolvimento utilizando"),
                F.lit("IA para desenvolvimento")
            ),

            # Aplicação de IA em produtos, serviços ou soluções destinadas
            # aos clientes.
            F.when(
                F.col("tipo_uso_ia_empresa_limpo")
                 .contains("diferenciação de produtos oferecidos"),
                F.lit("Produtos / clientes")
            ),

            # Aplicação de IA para eficiência operacional e produtividade.
            F.when(
                F.col("tipo_uso_ia_empresa_limpo")
                 .contains("eficiencia de processos internos"),
                F.lit("Processos internos / produtividade")
            ),

            # Organizações em estágio inicial, com poucos casos de uso isolados.
            F.when(
                F.col("tipo_uso_ia_empresa_limpo")
                 .contains("poucos casos de uso são isolados"),
                F.lit("Baixa maturidade / casos isolados")
            ),

            # Organizações nas quais IA já representa uma frente central
            # do modelo de negócio.
            F.when(
                F.col("tipo_uso_ia_empresa_limpo")
                 .contains("principal frente do negócio"),
                F.lit("IA como frente principal do negócio")
            ),

            # Respondentes que declararam não possuir informação suficiente
            # para avaliar a adoção de IA da empresa.
            F.when(
                F.col("tipo_uso_ia_empresa_limpo")
                 .contains("Não sei opinar"),
                F.lit("Não sabe opinar")
            )
        )
    )

    # Mantém apenas categorias efetivamente identificadas.
    .withColumn(
        "categorias_ia_empresa",
        F.filter(
            F.col("categorias_ia_empresa"),
            lambda x: x.isNotNull()
        )
    )
)


# ============================================================
# 7. EXPLODE: UMA LINHA POR RESPONDENTE + CATEGORIA EMPRESARIAL
# ============================================================

df_empresa_normalizado = (
    df_empresa
    .select(
        "respondente_id",
        "ano_pesquisa",
        F.explode("categorias_ia_empresa")
         .alias("categoria_uso_ia_empresa")
    )
)


# ============================================================
# 8. VALIDAÇÃO DA NORMALIZAÇÃO DE IA NAS EMPRESAS
# ============================================================

print("=== DISTRIBUIÇÃO IA EMPRESA NORMALIZADA ===")

(
    df_empresa_normalizado
    .groupBy(
        "ano_pesquisa",
        "categoria_uso_ia_empresa"
    )
    .agg(
        F.countDistinct("respondente_id")
         .alias("respondentes")
    )
    .orderBy(
        "ano_pesquisa",
        F.desc("respondentes")
    )
    .show(50, truncate=False)
)

# Lista respostas preenchidas que não foram classificadas.
# Após a calibração das regras, o resultado esperado é zero.
print("=== IA EMPRESA NÃO CLASSIFICADA ===")

(
    df_empresa
    .filter(
        F.col("tipo_uso_ia_empresa_limpo").isNotNull()
    )
    .filter(
        F.size("categorias_ia_empresa") == 0
    )
    .groupBy(
        "ano_pesquisa",
        "tipo_uso_ia_empresa_limpo"
    )
    .count()
    .orderBy(F.desc("count"))
    .show(30, truncate=False)
)


# ============================================================
# 9. GRAVAÇÃO DAS TABELAS ANALÍTICAS
# ============================================================

# As duas perguntas são gravadas em datasets separados.
# Isso evita multiplicação cartesiana em análises futuras, já que ambas
# possuem cardinalidade de um-para-muitos em relação ao respondente.

print("=== GRAVANDO USO IA GENERATIVA ===")

(
    df_ia_normalizado
    .dropDuplicates()
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(
        "s3://dados-fase3/gold/analytics/uso_ia_generativa/"
    )
)

print("=== GRAVANDO USO IA EMPRESA ===")

(
    df_empresa_normalizado
    .dropDuplicates()
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(
        "s3://dados-fase3/gold/analytics/uso_ia_empresa/"
    )
)

print("=== GRAVAÇÃO CONCLUÍDA ===")

# Finaliza formalmente o job no AWS Glue.
job.commit()
