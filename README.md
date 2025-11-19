FinDashPro ML-Max v3.2.0
Sistema di trading algoritmico ibrido con supporto IBKR
Budget: €0-130/mese | Timeline: 16 settimane

QUICKSTART:
1. cp .env.example .env
2. docker compose up -d vault redis postgres grafana
3. docker compose run --rm app python scripts/init_vault.py
4. docker compose up -d app ibkr-gateway
5. Accedi a http://localhost:8501 (dashboard)

STRUTTURA:
- fdp/           Core application package
- tests/         Test suite completo
- dashboard.py   Streamlit UI
- grafana/       Dashboard monitoring
- init.sql       Schema database
- prometheus.yml Config metrics

REQUISITI:
- Docker & Docker Compose
- Account IBKR Pro (opzionale, per live trading)
- Python 3.11+

SICUREZZA:
- Vault per secrets management
- Input sanitization su tutti gli endpoint
- Audit log immutabile
- Kill switch remoto su S3

COMPLIANCE:
- Modalità solo-notifiche: 100% legale per privati
- Auto-trading: richiede notifica CONSOB solo se HFT commerciale
- Fiscal reporting incluso per Italia (26%)

COSTI:
- Gratis: Paper trading, dati ritardati, self-hosted
- Pro: €90-130/mese (real-time, backup, commissioni)

DOCUMENTAZIONE:
- Sphinx docs/ (make html)
- API docs: http://localhost:8000/docs (quando avviato)
