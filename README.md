# League Data ETL

Um pipeline ETL (Extract, Transform, Load) para extração, processamento e análise de dados de partidas de League of Legends em tempo real.

## 📋 Descrição

Este projeto coleta dados de jogadores Challenger e suas partidas através da API Riot Games, processa os dados usando Apache Kafka e armazena as informações enriquecidas em um banco de dados PostgreSQL.

## 🏗️ Arquitetura

O projeto utiliza uma arquitetura de microserviços containerizada com os seguintes componentes:

```
┌─────────────────┐
│   Riot API      │
│   (Extração)    │
└────────┬────────┘
         │
┌─────────▼────────────┐
│  Redis              │
│  (Cache/Estado)     │
└────────┬────────────┘
         │
┌────────▼─────────────┐
│  Apache Kafka       │
│  (Event Streaming)  │
└────────┬────────────┘
         │
┌────────▼─────────────┐
│  PostgreSQL         │
│  (Data Warehouse)   │
└─────────────────────┘
```

### Serviços

- **Redis**: Gerenciador de estado e cache
- **Zookeeper**: Coordenador do Kafka
- **Apache Kafka**: Plataforma de streaming de eventos
- **PostgreSQL**: Banco de dados para armazenamento persistente

## 📁 Estrutura do Projeto

```
league_data_etl/
├── config/              # Configurações do projeto
├── data/                # Arquivos de dados
│   ├── matchs_ids.json
│   ├── players.json
│   └── timeline.json
├── notebook/            # Análise de dados
│   └── analysis_data.ipynb
├── src/                 # Código fonte
│   ├── consume_matches.py    # Consumer Kafka
│   └── extract_data.py       # Extrator de dados da API
├── docker-compose.yml   # Configuração dos serviços
├── main.py              # Entrypoint da aplicação
├── pyproject.toml       # Dependências do projeto
└── README.md            # Este arquivo
```

## 🚀 Configuração e Instalação

### Pré-requisitos

- Python 3.12+
- Docker e Docker Compose
- Chave API do Riot Games

### 1. Clonar o Repositório

```bash
git clone <repository-url>
cd league_data_etl
```

### 2. Configurar Variáveis de Ambiente

Crie um arquivo `.env` na pasta `config/`:

```bash
# config/.env
API_KEY=sua_chave_riot_api_aqui
```

### 3. Instalar Dependências

```bash
# Criar ambiente virtual (opcional)
python -m venv .venv
source .venv/bin/activate  # No Windows: .venv\Scripts\activate

# Instalar dependências
pip install -e .
```

### 4. Iniciar os Serviços

```bash
docker-compose up -d
```

Isso iniciará:
- Redis na porta 6379
- Zookeeper na porta 2181
- Kafka na porta 9092
- PostgreSQL na porta 5433

## 📊 Fluxo de Dados

### Extração (`extract_data.py`)

1. **all_challengers_players()**: Busca todos os jogadores do tier Challenger na API Riot
   - Realiza requisição para `https://br1.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5`
   - Extrai PUUIDs e armazena em `data/players.json`

2. **get_match_ids()**: Recupera IDs de partidas para cada jogador
   - Utiliza os PUUIDs dos jogadores para buscar histórico de partidas

### Processamento

- Dados são processados e transformados usando Pandas/PySpark
- Enriquecimento de dados com análises de winrate e estatísticas

### Carregamento (`consume_matches.py`)

1. **inciar_consumidor()**: Consumer Kafka que:
   - Consome mensagens do tópico `lol_winrates_enriched`
   - Conecta ao banco PostgreSQL
   - Insere dados na tabela `partidas_enriquecidas`
   - Evita duplicatas com `ON CONFLICT` constraint

## 🔌 Dependências Principais

- **redis**: Cache e gerenciamento de estado
- **requests**: Requisições HTTP à API Riot
- **python-dotenv**: Carregamento de variáveis de ambiente
- **pandas**: Manipulação e análise de dados
- **kafka-python**: Cliente Kafka
- **pyspark**: Processamento distribuído de dados
- **psycopg2**: Driver PostgreSQL

## 📝 Variáveis de Ambiente

| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `API_KEY` | Chave de autenticação Riot Games | Obrigatório |
| `RIOT_API_KEY` | Alternativa para API_KEY | - |

## 🗄️ Banco de Dados

### Credenciais PostgreSQL

- **Host**: localhost
- **Port**: 5433
- **Database**: lol_data
- **User**: postgres
- **Password**: 3695

### Tabelas

- `partidas_enriquecidas`: Contém dados de partidas enriquecidas
  - match_id
  - duracao_segundos
  - time_vencedor
  - alma_conquistada (dragon_soul)

## 📈 Análise de Dados

O projeto inclui um Jupyter Notebook para análise dos dados coletados:

```bash
jupyter notebook notebook/analysis_data.ipynb
```

## 🛠️ Desenvolvimento

### Executar Aplicação

```bash
python main.py
```

### Visualizar Logs

```bash
# Redis
docker-compose logs redis

# Kafka
docker-compose logs kafka

# PostgreSQL
docker-compose logs postgres-db
```

### Parar os Serviços

```bash
docker-compose down
```

## 🔍 Endpoints e Tópicos

### API Riot Games

- **Region**: br1 (Brasil)
- **Routing**: americas
- **Queue**: RANKED_SOLO_5x5

### Tópicos Kafka

- `lol_winrates_enriched`: Dados enriquecidos de partidas

## ⚠️ Notas Importantes

- A API Riot Games possui rate limiting, respeite os limites
- As credenciais do banco de dados são apenas para desenvolvimento
- Para produção, use variáveis de ambiente seguras
- O banco PostgreSQL está exposto na porta 5433 para desenvolvimento

## 📚 Referências

- [Riot Games Developer Portal](https://developer.riotgames.com/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Redis Documentation](https://redis.io/docs/)

## 👤 Autor

League Data ETL Project

## 📄 Licença

Este projeto é fornecido como está para fins de desenvolvimento e análise.
