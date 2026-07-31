# olist-customer-behavior-pipeline

> Verification: `WTC-CJ87HLMM`

# Olist Customer Behavior Pipeline

End-to-end data pipeline built on the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce): 
```
  ingestion → data quality → transforms → customer behavior features (RFM, churn proxy, delivery experience) → serving layer.
```
Built as my Data Engineering elective project at [WeThinkCode_](https://www.wethinkcode.co.za/).

🎥 **Demo video:** [To be added]

---

## Why this project

I wanted a pipeline built on data that actually supports customer-behavior analysis, not synthetic data, and not a dataset where the "customer behavior" angle has to be invented. 
Olist gives real, relational, appropriately messy e-commerce data: 
```
orders
customers
payments
reviews and
delivery logistics across ~100k orders.
```
---

## Architecture

```
Kaggle CSVs (9 files)
        │
        ▼
   INGESTION (raw layer, Airflow)
        │
        ▼
   DATA QUALITY (referential integrity, null checks)
        │
        ▼
   TRANSFORM (staging → marts, joins/cleaning)
        │
        ▼
   FEATURES (RFM, delivery experience, churn proxy)
        │
        ▼
   SERVING (dashboard / analytics table)
```

Full breakdown in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Tech stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 3.x (TaskFlow API, dynamic task mapping) |
| Storage | PostgreSQL |
| Transform | [SQL via PostgresHook] |
| Data quality | [Great Expectations] |
| Serving | [Power BI] |
| Language | Python 3.x |

---

## Repo structure

```
.
├── dags/                 # Airflow DAGs
├── sql/                  # staging/mart models 
├── data/                 # raw CSVs 
├── docs/
│   ├── ARCHITECTURE.md   # detailed design + diagrams
│   └── DECISIONS.md      # key design decisions and tradeoffs
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Key design decisions

- **Why Olist over synthetic/market data:** [TODO]
- **Churn proxy definition:** [e.g. "no repeat order within 90 days of last purchase" and why]
- **Why plain SQL vs dbt**: [Reasoning]
- **Data quality strategy:** [what you check, and why the DAG fails loudly rather than logging and continuing]

---

## Setup

```bash
# 1. Clone
git clone https://github.com/yamkela-macwili/olist-customer-behavior-pipeline.git
cd olist-customer-behavior-pipeline

# 2. Download the dataset from Kaggle and place CSVs in data/
#    https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

# 3. Start services
docker-compose up -d

# 4. Trigger the DAG via the Airflow UI (localhost:8080) or CLI
airflow dags trigger olist_customer_behavior_pipeline
```

---

## Status / Roadmap

- [ ] Ingestion (raw layer) — all 9 tables loading via dynamic task mapping
- [ ] Data quality checks — referential integrity, null thresholds
- [ ] Transform layer — fact/dim tables
- [ ] Feature layer — RFM, delivery experience, churn proxy
- [ ] Serving layer — dashboard
- [ ] Demo video recorded and linked

---

## License

MIT
