# Pipeline de Processamento de Dados de Livros

Esta pipeline executa ETL (Extract, Transform, Load) completo nos dados de livros, realizando limpeza básica e feature engineering avançado.

## Estrutura da Pipeline

### 📁 Arquivos Principais

- `data_types.py` - Modelos Pydantic e schemas de validação
- `cleaning_pipeline.py` - Pipeline de limpeza básica dos dados
- `feature_pipeline.py` - Pipeline de feature engineering
- `main_pipeline.py` - Orquestração completa da pipeline
- `run_pipeline.py` - Script executável com CLI

## 🧹 Pipeline de Limpeza (Fase 1)

**Entrada:** `data/raw/all_books_with_images.csv`  
**Saída:** `data/processed/books_processed.csv`

### Operações Realizadas:

1. **Verificação de nulos** - Valida integridade dos dados
2. **Criação de ID único** - Adiciona coluna `id` com identificadores únicos
3. **Limpeza de categorias** - Substitui categorias problemáticas ('Add a comment', 'Default') por 'Outros'
4. **Transformação availability** - Converte 'yes'/'no' para 1/0
5. **Validação final** - Garante consistência dos dados processados

### Resultados da Limpeza:
- ✅ 1000 registros processados sem nulos
- 🏷️ 219 categorias problemáticas limpas
- 🔢 1000 IDs únicos criados

## ⚙️ Pipeline de Feature Engineering (Fase 2)

**Entrada:** `data/processed/books_processed.csv`  
**Saída:** `data/features/books_features.csv`

### Features Criadas (59 total):

#### 1. **Categorização de Preços**
- `price_range`: Baixo (≤20), Médio (20-40), Alto (40-50), Premium (>50)

#### 2. **Features de Título** (6 features)
- `has_subtitle`: Título contém ':' (307 livros)
- `has_series`: Título contém '(' (345 livros) 
- `starts_with_the`: Começa com 'The' (269 livros)
- `title_length`: Comprimento em caracteres (média: 39.1)
- `title_word_count`: Número de palavras (média: 6.7)
- `has_numbers`: Contém números (365 livros)

#### 3. **Categorização de Ratings**
- `rating_category`: Muito Baixo/Baixo/Médio/Alto/Muito Alto

#### 4. **Níveis de Stock**
- `stock_level`: Baixo (1-5), Médio (6-15), Alto (16+)

#### 5. **Score de Popularidade**
- `popularity_score`: Combinação de rating e stock (0.154-0.973)
- Fórmula: `(rating/5) × 0.7 + (stock_norm) × 0.3`

#### 6. **One-Hot Encoding de Categorias** (49 colunas)
- `category_*`: Uma coluna binária para cada categoria única
- Exemplo: `category_fantasy` (48 livros), `category_nonfiction` (110 livros)

## 🚀 Como Executar

### Opções de Execução:

```bash
# Pipeline completa (limpeza + features)
python scripts/run_pipeline.py

# Apenas limpeza
python scripts/run_pipeline.py --cleaning-only

# Apenas features (requer dados processados)
python scripts/run_pipeline.py --features-only

# Com configuração customizada
python scripts/run_pipeline.py --config config.json

# Mostrar informações
python scripts/run_pipeline.py --info

# Criar arquivo de configuração padrão
python scripts/run_pipeline.py --create-config config.json
```

### Configuração Padrão:

```json
{
  "input_file": "data/raw/all_books_with_images.csv",
  "processed_output": "data/processed/books_processed.csv", 
  "features_output": "data/features/books_features.csv",
  "default_category": "Outros",
  "problematic_categories": ["Add a comment", "Default"]
}
```

## 📊 Resultados da Última Execução

- **Registros processados:** 1000
- **Tempo de execução:** 0.42s  
- **Features criadas:** 59
- **Categorias limpas:** 219
- **Distribuição de preços:**
  - Médio (20-40): 401 livros (40.1%)
  - Alto (40-50): 205 livros (20.5%)
  - Premium (>50): 198 livros (19.8%)
  - Baixo (≤20): 196 livros (19.6%)

## 🔧 Dependências

- `polars` - Processamento eficiente de dados
- `pydantic` - Validação e tipagem de dados
- `pathlib` - Manipulação de caminhos
- `uuid` - Geração de IDs únicos

## 📈 Próximos Passos

Os dados estão prontos para:
- ✅ Análise exploratória avançada
- ✅ Modelagem preditiva de preços
- ✅ Sistema de recomendação de livros
- ✅ API de consulta de dados

## 🎯 Qualidade dos Dados

- **0 valores nulos** - Dataset íntegro
- **1000 IDs únicos** - Identificação consistente
- **59 features** - Rico conjunto para ML
- **Validação completa** - Dados confiáveis para análise

---

**Desenvolvido para Tech Challenge**  
*Pipeline otimizada com Polars + Pydantic*