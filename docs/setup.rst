Setup & Installation Guide
==========================

Quick Start (5 minutes)
-----------------------

1. **Clone repository:**

   .. code-block:: bash
      
      git clone https://github.com/findashpro/findashpro.git
      cd findashpro

2. **Configure environment:**

   .. code-block:: bash
      
      cp .env.example .env
      nano .env  # Add your API keys

   Required keys (free tier):
   - `FMP_API_KEY` - Financial Modeling Prep
   - `FINNHUB_API_KEY` - Finnhub
   - `ALPHA_VANTAGE_API_KEY` - Alpha Vantage

3. **Start services:**

   .. code-block:: bash
      
      docker-compose up -d

4. **Access services:**
   - Dashboard: http://localhost:8501
   - Grafana: http://localhost:3000 (admin/admin)
   - Prometheus: http://localhost:9090

Configuration
-------------

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 40 30
   :header-rows: 1

   * - Variable
     - Description
     - Example
   * - ``FDP_EXECUTION_MODE``
     - alert_only, paper, ibkr, alpaca
     - paper
   * - ``FMP_API_KEY``
     - Financial Modeling Prep API key
     - your_free_key
   * - ``FDP_DAILY_API_BUDGET``
     - Max spend per day (€)
     - 5.0
   * - ``FDP_TIER_POLYGON``
     - free | premium | disabled
     - free
   * - ``FDP_MAX_TICKERS``
     - Universe size
     - 500
   * - ``FDP_MIN_CONFIDENCE``
     - ML signal threshold
     - 0.75

Docker Deployment
-----------------

Build custom image:

.. code-block:: bash
   
   docker build -t findashpro:latest .

Run without compose:

.. code-block:: bash
   
   docker run -d \
     --env-file .env \
     -p 8501:8501 \
     -p 9090:9090 \
     --name findashpro \
     findashpro:latest

Kubernetes (K8s)
----------------

Helm chart values:

.. code-block:: yaml
   
   replicaCount: 3
   image:
     repository: findashpro/findashpro
     tag: "3.1.4"
   env:
     FDP_EXECUTION_MODE: "paper"
     FMP_API_KEY: "your_key"
   resources:
     limits:
       cpu: 2
       memory: 4Gi

Apply:

.. code-block:: bash
   
   helm install findashpro ./helm/

Production Checklist
--------------------

✅ Secrets management (HashiCorp Vault o AWS Secrets Manager)  
✅ TLS certificates per dashboard  
✅ Network policies K8s  
✅ Prometheus alerting rules  
✅ Backup PostgreSQL giornaliero  
✅ Log aggregation (ELK stack)  
✅ Kill switch testato  
✅ Disaster recovery plan documentato

Troubleshooting
---------------

**Redis connection failed:**
   Check ``redis://localhost:6379`` accessibile

**Database connection failed:**
   Verifica ``DATABASE_URL`` e che Postgres sia running

**Out of budget:**
   Aumenta ``FDP_DAILY_API_BUDGET`` o disabilita provider premium

**No signals generated:**
   Controlla log: ``docker logs findashpro_orchestrator_1 | grep -i signal``
