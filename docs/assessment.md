# Avaliação inicial e plano técnico

Data da avaliação: 2026-10-22.

## 1. Estado encontrado

| Item | Resultado da inspeção |
|---|---|
| Repositório | `~/enterprise-lakehouse` vazio (sem código, sem git, sem `.codegraph/` — CodeGraph não se aplica) |
| SO | Ubuntu (WSL2) em Windows 11; 8 vCPUs, 23 GiB RAM, 673 GiB livres |
| Python | 3.12.3 (sistema); `uv` 0.12.5 disponível |
| Java | OpenJDK 17.0.20.1 |
| Docker | Engine 29.8.0, Compose v5.5.1 |
| Portas já ocupadas no host | 5432, 5433, 8080, 8501, 9000, 9001, 9100, 9101, 16379, 18080, 19000, 29092 (outros projetos) |
| MinIO | `minio/minio` **não existe mais** no Docker Hub; `quay.io/minio/*` responde 401. A única imagem obtida foi `bitnamilegacy/minio:2025.7.23` |

Evidência da matriz de versões (smoke test executado): PySpark 3.5.7 + `delta-spark` 3.3.2 inicializam em Java 17 / Python 3.12,
gravam Delta com `userMetadata` no commit e `try_to_timestamp('2024-02-30 10:00:00')` retorna `NULL`.

## 2. Versões escolhidas

| Componente | Versão | Motivo |
|---|---|---|
| Python | 3.12 | Disponível no host e suportado por Spark 3.5.x, Airflow 3.3 e FastAPI |
| PySpark | 3.5.7 | Linha 3.5 madura; é a linha para a qual Delta 3.3 é compilado |
| Delta Lake | 3.3.2 (`delta-spark_2.12`) | Par oficial do Spark 3.5 |
| hadoop-aws / AWS SDK | 3.3.4 / 1.12.262 | Mesma versão do Hadoop embarcado no PySpark 3.5 (misturar versões quebra o S3A) |
| Java | 17 | Suportado pelo Spark 3.5; já instalado |
| Airflow | 3.3.2 (imagem oficial `-python3.12`) | Linha 2.x está fora de suporte; 3.3.2 já roda neste host em outro projeto |
| PostgreSQL | 17-alpine | Metadados do Airflow e da plataforma (bancos e usuários separados) |
| MinIO | `bitnamilegacy/minio:2025.7.23` | Única imagem disponível; ver ADR-0006 |
| delta-rs (`deltalake`) | 1.x | Leitura de Gold pela API sem JVM |

## 3. Arquitetura proposta (resumo)

* **Landing** (S3/MinIO ou diretório local) recebe arquivos CSV, JSON e Parquet por entidade.
* **Ingestão** (Python puro, sem Spark) descobre arquivos, calcula SHA-256 do conteúdo, detecta duplicados,
  agrupa em lotes e registra tudo no PostgreSQL.
* **Bronze** (Spark) lê os arquivos planejados, preserva os valores brutos como texto, adiciona linhagem
  (lote, execução, arquivo, hash) e faz um único commit Delta por lote/entidade.
* **Silver** (Spark) tipa, normaliza, valida linha a linha, verifica integridade referencial, deduplica por chave de negócio
  e faz `MERGE` idempotente; rejeitados vão para `silver/_rejects` com motivos.
* **Gold** (Spark) reconstrói dimensões, fato e agregados a partir de versões fixadas da Silver e só publica
  (registro em `gold_publications`) depois de passar nos controles bloqueantes (Write-Audit-Publish).
* **Qualidade e reconciliação**: catálogo único de regras (linha e conjunto), resultados persistidos por lote.
* **Airflow** chama a CLI `lakehouse` (subprocesso por tarefa, JVM isolada); DAGs não contêm lógica de dados.
* **API FastAPI** lê metadados do PostgreSQL e a versão Gold publicada via delta-rs.
* **Observabilidade**: logs JSON com correlação; métricas Prometheus derivadas do repositório de metadados; dashboard Grafana.

## 4. Decisões técnicas principais

1. Identidade de arquivo = `(entidade, SHA-256 do conteúdo)`, não o nome (ADR-0003).
2. Consistência entre Delta e PostgreSQL por recuperação, não por transação distribuída: commits Delta carregam `batch_id`
   em `userMetadata`; uma reexecução verifica o histórico Delta antes de regravar (ADR-0004).
3. Silver idempotente por construção: `MERGE` condicionado a `(updated_at, batch_seq)` e rejeitados substituídos por lote (ADR-0005).
4. Schema: Bronze com contrato fixo; colunas novas são capturadas em `_extra_columns` e registradas; ausência de coluna
   obrigatória coloca o arquivo em quarentena. Sem `mergeSchema` automático (ADR-0007).
5. Gold com reconstrução completa e publicação por versão (ADR-0008).

## 5. Riscos identificados

| Risco | Mitigação |
|---|---|
| Imagem MinIO congelada e sem correções de segurança | Uso apenas local; acesso só via API S3; troca documentada |
| Download de jars Maven (Delta, hadoop-aws) no primeiro uso | Pré-carregados na imagem do Airflow; cache Ivy no CI |
| Relógio do WSL saltando invalida tokens JWT do Airflow 3 | `AIRFLOW__API_AUTH__JWT_LEEWAY=60` (problema já observado neste host) |
| Spark local dentro do container do scheduler disputa memória | `local[2]`, driver de 2 GiB, `max_active_runs=1` |
| Conflitos de portas com outros projetos | Todas as portas do host parametrizadas no `.env` |
| Histórico Delta truncado após `logRetentionDuration` (30 dias) | Verificação de recuperação usa histórico e, como fallback, os dados (`_batch_id`) |

## 6. Estrutura de diretórios

```
src/lakehouse/        pacote Python (layout src)
  common/             Spark, armazenamento, falhas injetáveis
  contracts.py        contratos de dados por entidade
  generator/          gerador sintético de varejo
  ingestion/          descoberta e planejamento de lotes (sem Spark)
  metadata/           modelo e repositório (SQLAlchemy Core)
  pipelines/          bronze/, silver/, gold/, runner
  quality/            framework, catálogo de regras, reconciliação, anomalias
  observability/      logs JSON, métricas Prometheus
  api/                FastAPI
dags/                 DAGs Airflow (apenas orquestração)
config/               configuração YAML
infrastructure/       Dockerfiles, init SQL, Prometheus, Grafana
tests/{unit,integration,resilience}
scripts/              utilitários (benchmark, verificação de ambiente)
docs/                 documentação e ADRs
```

## 7. Plano de implementação

Fases 1–5 conforme o enunciado; ao fim de cada fase, testes e verificações registrados em `docs/acceptance.md`.
