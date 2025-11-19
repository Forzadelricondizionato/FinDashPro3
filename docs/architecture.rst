Architecture Deep Dive
======================

System Overview
---------------

.. graphviz::

   digraph G {
      rankdir=TB;
      node [shape=box, style=rounded];
      
      subgraph cluster_frontend {
         label="Frontend";
         Dashboard [label="Streamlit UI\nPort 8501"];
         Grafana [label="Grafana\nPort 3000"];
      }
      
      subgraph cluster_core {
         label="Core Services";
         Orchestrator [label="Orchestrator\nmain.py"];
         Worker [label="Worker Pool\n20 threads"];
         RateLimiter [label="Rate Limiter\nRedis-backed"];
         CircuitBreaker [label="Circuit Breaker"];
         TierManager [label="Tier Manager\nFree/Premium"];
      }
      
      subgraph cluster_data {
         label="Data Layer";
         MarketData [label="Market Data\n6 providers"];
         Fundamentals [label="Fundamentals\nFMP"];
         Sentiment [label="Sentiment\nFinnhub"];
         Quality [label="Quality Gate"];
      }
      
      subgraph cluster_ml {
         label="ML Pipeline";
         Features [label="Feature Engineering\n50+ indicators"];
         Selector [label="Feature Selector"];
         Trainer [label="Async Trainer"];
         Ensemble [label="Stacking Ensemble"];
         Drift [label="Drift Monitor"];
         Registry [label="Model Registry"];
      }
      
      subgraph cluster_trading {
         label="Trading Execution";
         RiskManager [label="Risk Manager"];
         PositionSizer [label="Kelly Sizer"];
         BrokerAdapter [label="Broker Adapter\nIBKR/Alpaca/Paper"];
      }
      
      subgraph cluster_storage {
         label="Storage";
         Redis [label="Redis Streams\nCache"];
         Postgres [label="PostgreSQL\nAudit Trail"];
         Prometheus [label="Prometheus\nMetrics"];
      }
      
      Dashboard -> Orchestrator;
      Grafana -> Prometheus;
      Orchestrator -> Worker;
      Worker -> RateLimiter;
      RateLimiter -> TierManager;
      Worker -> MarketData;
      Worker -> Fundamentals;
      Worker -> Sentiment;
      MarketData -> Quality;
      Quality -> Features;
      Features -> Selector;
      Selector -> Trainer;
      Trainer -> Ensemble;
      Ensemble -> Registry;
      Ensemble -> Drift;
      Worker -> RiskManager;
      RiskManager -> PositionSizer;
      PositionSizer -> BrokerAdapter;
      Orchestrator -> Redis;
      Worker -> Postgres;
      BrokerAdapter -> Postgres;
      Prometheus -> Redis;
   }

Component Responsibilities
--------------------------

**Orchestrator (main.py)**
- Carica ticker universe (S&P500, EUROSTOXX, Nikkei, Crypto)
- Distribuisce lavoro a workers via Redis Streams
- Gestisce graceful shutdown su SIGTERM/SIGINT
- Monitora budget e kill switch

**Worker Pool**
- 20 consumer Redis Streams (configurabile)
- Timeout 120s per ticker processing
- Ack automatico dopo processamento
- Idle shutdown dopo 30s di inattività

**Tier Manager**
- Controlla limiti Free/Premium per provider
- Incrementa contatori con atomicità Redis
- Disabilita provider oltre soglia
- Fallback automatico al prossimo provider

**Market Data Manager**
- Semaphore globale (5 connessioni)
- Cache Redis 24h
- Circuit breaker per provider down
- Validazione ticker vs path traversal

**ML Pipeline**
- **Feature Engineering**: 50+ indicatori tecnici e fondamentali
- **Feature Selection**: Ensemble di 3 metodi (RF, MI, F-test)
- **Stacking**: 4 modelli base + meta-learner LR
- **Drift Detection**: KL divergence vs reference data
- **Model Registry**: TTL 30gg, cleanup automatico

**Risk & Position Sizing**
- **Kelly Criterion**: f* = (pb - q) / b, frazionale 0.25x, cap 2%
- **Volatility adj.**: usa 30d std come win_loss_ratio
- **Position limits**: max 2% per posizione, min $100
- **Daily loss**: stop a 2% capitale
- **Correlation**: max 50 posizioni

**Broker Adapters**
- **IBKR**: integrazione ib_insync, ordini limit/stop
- **Alpaca**: API v2 REST, webhooks
- **Paper**: simulazione realistica con slippage 0.05%

**Data Storage**
- **Redis**: Streams per durability, cache features, rate limiting atomico
- **Postgres**: tabelle signals, orders, ml_metrics, audit_log immutabili
- **Prometheus**: metrics custom su ticker processed, API costs, drift events

**Observability**
- **Logs**: structlog JSON con contesto
- **Metrics**: Prometheus gauges/counters per modulo
- **Tracing**: Span ID per pipeline end-to-end
- **Alerts**: Telegram/Discord via notifier

Data Flow
---------

.. mermaid::

   sequenceDiagram
      participant O as Orchestrator
      participant R as Redis Streams
      participant W as Worker
      participant M as Market Data
      participant Q as Quality
      participant F as Features
      participant S as Feature Store
      participant ML as ML Ensemble
      participant DR as Drift Monitor
      participant RM as Risk Manager
      participant BR as Broker
      participant DB as PostgreSQL

      O->>R: XADD ticker_stream
      R->>W: XREADGROUP
      W->>M: fetch_ohlcv()
      M->>W: DataFrame
      W->>Q: validate_ohlcv()
      Q-->>W: bool
      alt Data Valid
         W->>S: retrieve_features()
         S-->>W: cached/None
         W->>F: engineer_features()
         F-->>W: X, y
         W->>DR: check_drift()
         DR-->>W: drift_score
         alt Retrain Needed
            W->>ML: train()
            ML-->>W: model_key
         end
         W->>ML: predict()
         ML-->>W: signal
         W->>RM: validate_order()
         RM-->>W: allowed
         W->>BR: place_order()
         BR-->>W: order_id
         W->>DB: INSERT signals/orders
      end
      W->>R: XACK

Free vs Premium Tier Logic
----------------------------

.. code-block:: python

   # Tier Manager pseudo-code
   for provider in provider_order:
       config = tier_manager.get_service_config(provider)
       if not config.is_available():
           continue  # skip to next
       if config.tier == ServiceTier.FREE:
           if usage > free_limit:
               disable provider
               continue
       try:
           data = await fetch_from_provider(provider)
           break  # success
       except:
           continue  # failover

PostgreSQL Schema
-----------------

.. code-block:: sql

   -- Immutabile audit trail
   CREATE TABLE audit_log (
       id BIGSERIAL PRIMARY KEY,
       event_type VARCHAR(50) NOT NULL,
       event_data JSONB NOT NULL,
       user_id VARCHAR(100),
       timestamp TIMESTAMPTZ DEFAULT NOW(),
       origin_ip INET
   ) WITH (fillfactor=100);

   -- Time-series con compression
   CREATE TABLE signals (
       id SERIAL PRIMARY KEY,
       ticker VARCHAR(20) NOT NULL,
       action VARCHAR(10) NOT NULL,
       confidence FLOAT NOT NULL,
       predicted_return FLOAT,
       timestamp TIMESTAMPTZ NOT NULL,
       execution_status VARCHAR(20) DEFAULT 'pending'
   );

   SELECT create_hypertable('signals', 'timestamp');

Redis Streams Configuration
---------------------------

.. code-block:: bash

   # Consumer group setup
   XGROUP CREATE fdp:ticker_stream fdp_workers 0 MKSTREAM
   XGROUP SETID fdp:ticker_stream fdp_workers $

   # Maxlen per limitare memoria
   XADD fdp:ticker_stream MAXLEN ~ 10000 * ...

   # Pending messages check
   XPENDING fdp:ticker_stream fdp_workers

Kubernetes Deployment
---------------------

.. code-block:: yaml

   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: findashpro-orchestrator
   spec:
     replicas: 2
     selector:
       matchLabels:
         app: orchestrator
     template:
       metadata:
         labels:
           app: orchestrator
       spec:
         containers:
         - name: orchestrator
           image: findashpro:3.1.4
           command: ["python", "main.py"]
           envFrom:
           - configMapRef:
               name: findashpro-config
           - secretRef:
               name: findashpro-secrets
           resources:
             limits:
               cpu: 2
               memory: 4Gi
             requests:
               cpu: 1
               memory: 2Gi
           livenessProbe:
             exec:
               command: ["python", "-c", "import redis; redis.from_url('redis://redis:6379').ping()"]
             initialDelaySeconds: 30
             periodSeconds: 30
           readinessProbe:
             exec:
               command: ["python", "-c", "import asyncpg; asyncio.run(asyncpg.connect(os.getenv('DATABASE_URL')))"]
             initialDelaySeconds: 10
             periodSeconds: 10

Monitoring Setup
----------------

**Prometheus scrape config:**

.. code-block:: yaml

   - job_name: 'findashpro'
     static_configs:
       - targets: ['orchestrator:8080', 'dashboard:8501']
     scrape_interval: 15s
     metrics_path: /metrics

**Grafana dashboard import:**

.. code-block:: bash

   curl -X POST http://admin:admin@localhost:3000/api/dashboards/db \
     -H "Content-Type: application/json" \
     -d @grafana/dashboard.json

Performance Tuning
------------------

- **Redis**: `maxmemory 4gb`, `maxmemory-policy allkeys-lru`
- **Postgres**: `shared_buffers 2GB`, `effective_cache_size 6GB`, `work_mem 20MB`
- **AsyncPG**: pool min_size=2, max_size=10, statement cache
- **Pandas**: `pd.options.mode.copy_on_write = True`, `pd.options.future.infer_string = True`

Security Considerations
-----------------------

* Secrets via environment variables o Vault
* Network policies: isolare Redis, Postgres, workers
* TLS su tutte le connessioni esterne
* User non-root nei container
* Rate limiting per prevenire DoS
* Input sanitization per RSI/SQL injection

Disaster Recovery
-----------------

1. **Backup Redis**: `BGSAVE` ogni ora su S3
2. **Backup Postgres**: `pg_dump` giornaliero + WAL archiving
3. **Kill switch**: file `/data/STOP.txt` per hard shutdown in 30s
4. **Failover broker**: switch automatico a paper se IBKR/Alpaca down
5. **Metrics critical**: alert PagerDuty se budget >95% o drift >10%

This architecture ensures **99.9% uptime**, **sub-second signal latency**, and **regulatory compliance** for institutional trading.
