FinDashPro ML-Max 3.1.4
========================

Enterprise-Grade AI Trading System
-----------------------------------

.. image:: _static/logo.png
   :width: 200
   :alt: FinDashPro Logo
   :align: center

.. raw:: html

   <p style="text-align: center; font-size: 1.2em;">
      <strong>Production-Ready | ML-Powered | Fully Documented</strong>
   </p>

.. grid:: 1 2 2 2
   :gutter: 2

   .. grid-item-card:: 🚀 Quick Start
      :link: setup.html
      :link-type: doc
      
      Setup in 5 minutes with Docker Compose

   .. grid-item-card:: 🧠 ML Models
      :link: api.html#ml-modules
      :link-type: doc
      
      Stacking ensemble with drift detection

   .. grid-item-card:: 📊 Dashboard
      :link: architecture.html#streamlit-ui
      :link-type: doc
      
      Real-time trading interface

   .. grid-item-card:: 🏗️ Architecture
      :link: architecture.html
      :link-type: doc
      
      Microservices with Redis Streams

Features
========

* **Multi-source market data** (Yahoo, Polygon, FMP, Finnhub, Tiingo, Alpha Vantage)
* **Free/Premium tier system** with automatic failover
* **Machine learning stacking ensemble** (4 models + meta-learner)
* **Kelly Criterion position sizing** with volatility adjustment
* **Risk management** (position limits, daily loss, correlation)
* **Drift monitoring** with Evidently AI integration
* **Immutabile audit trail** su PostgreSQL
* **Redis Streams** per processing robusto
* **Docker multi-stage build** con non-root user
* **Prometheus + Grafana** monitoring
* **Sphinx documentation** auto-generata

Installation
------------

.. code-block:: bash

   git clone https://github.com/findashpro/findashpro.git
   cd findashpro
   cp .env.example .env
   # Modifica le API keys in .env
   docker-compose up -d

API Reference
=============

.. toctree::
   :maxdepth: 2
   
   api

Architecture
============

.. toctree::
   :maxdepth: 2
   
   architecture

Setup Guide
===========

.. toctree::
   :maxdepth: 2
   
   setup

Changelog
=========

* **3.1.4**: Aggiunta documentazione Sphinx, ReadTheDocs integration
* **3.1.3**: Implementato tier system Free/Premium
* **3.1.2**: Fix Kelly Criterion, Redis Streams shutdown
* **3.1.1**: Produzione beta con ML monitoring

License
=======

Proprietary - FinDashPro Enterprise License

Support
=======

* 📧 Email: support@findashpro.com
* 🐛 Issues: https://github.com/findashpro/findashpro/issues
* 💬 Telegram: https://t.me/findashpro
