# 🧠 Agentic Data Pipeline for Automated Sentiment Analysis

A **production-grade, agentic data pipeline** that autonomously detects, heals, and processes noisy real-world text data before performing **LLM-powered sentiment analysis at scale**.

This project is designed to demonstrate **how LLMs should be operationalized in real systems**—with strong data engineering fundamentals, resilience, and observability.

---

## 🚀 Business Problems This Project Solves

Most sentiment analysis projects stop at model inference.  
This one focuses on **system reliability, data quality, and scalability**—the real challenges in production.

**What this proves:**
- You can design resilient data pipelines
- You understand real-world data quality problems
- You know how to integrate LLMs safely
- You think like a production data engineer, not a notebook ML engineer

---

## 🔍 What Is Sentiment Analysis?

**Sentiment analysis** is an NLP task that classifies text into:
- **POSITIVE**
- **NEGATIVE**
- **NEUTRAL**

along with a **confidence score**.

In this pipeline:
- Raw Yelp reviews are validated and repaired
- Only clean, safe text is sent to the LLM
- Failures degrade gracefully instead of breaking the pipeline

---

## 🧠 Agentic & Self-Healing Pipeline Design

This pipeline behaves like an **autonomous agent**:

> Observe → Diagnose → Heal → Analyze → Improve


**Observe:** Continuously monitor pipeline health, data freshness, and SLA breaches using metrics and alerts.

**Diagnose:** Automatically identify failure types (data quality, schema drift, infra, or dependency issues).

**Heal:** Trigger automated remediation such as task retries, backfills, or dependency realignment.

**Analyze:** Capture incident metadata and execution logs for post-failure analysis.

**Improve:** Apply learnings to harden validations, optimize retries, and prevent repeat failures.

## 🩺 How Self-Healing Works

Before LLM inference, every record is validated and repaired using deterministic rules.

### Healing Rules Implemented

| Data Issue | Detection | Healing Action | Purpose |
|----------|----------|---------------|--------|
| Missing / Empty Text | `None` or whitespace | Insert placeholder text | Prevents inference failure |
| Special Characters Only | No alphanumeric chars | Replace with marker | Avoids garbage predictions |
| Excessively Long Text | > 2000 chars | Truncate safely | Controls cost & latency |
| Invalid Data Types | Non-string input | Convert to string | Enforces schema safety |

Each record tracks:
- `was_healed`
- `error_type`
- `action_taken`

This provides **full observability and auditability**.

---

## 🧱 Data Engineering Concepts Demonstrated

This project intentionally showcases **core data engineering skills**.

### Batch Processing
- Offset + batch size ingestion
- Memory-safe, restartable execution
- Scales to millions of records

### Idempotency
- Deterministic healing logic
- Safe retries without duplication

### Fault Tolerance
- Task-level retries
- API retry logic
- Neutral fallback predictions

### Graceful Degradation
- Partial failures do not crash the system
- Analytics continue with reduced fidelity

### Orchestration
- Explicit DAG dependencies
- Clear execution flow
- Timeout and retry management

### Observability
- Structured logging
- Healing metadata
- Record-level lineage

---

## 🏗️ Architecture Overview

Source Data (File / S3)
↓
Batch Loader (Offset-based)
↓
Self-Healing Engine
↓
LLM Sentiment Analysis (OpenAI)
↓
Enriched Output with Metadata

yaml
Copy code

- **Control Plane:** Apache Airflow
- **Data Plane:** Stateless batch processing

---

## 🛠️ Tech Stack & Rationale

### Apache Airflow
**Why:** Industry-standard workflow orchestrator  
**Role:** Scheduling, retries, task dependencies, observability

---

### PostgreSQL (Airflow Metadata Database)
**Why:** Reliable state management  
**Role:** Stores DAG state, task history, retries  
**Note:** Not used as an analytics database

---

### OpenAI (`gpt-3.5-turbo`)
**Why:** Fast, cost-effective, reliable for sentiment classification  
**Usage Pattern:**
- Deterministic prompts
- Low temperature
- Strict JSON parsing with fallback logic

---

### Docker (Production-Ready)
**Why:** Environment consistency and reproducibility  
**Role:** Containerized deployment for Airflow and dependencies

---

### Source Storage (File / S3-Compatible)
**Why:** Immutable, scalable source of truth  
**Role:** Batch ingestion pattern used in real data platforms

---

## ⚙️ Pipeline Execution Flow

### DAG Tasks
1. **Load Reviews** – Offset-based batch ingestion
2. **Heal Reviews** – Data validation and repair
3. **Analyze Sentiment** – LLM inference with retries

Each task is:
- Stateless
- Retry-safe
- Fully observable

---

## 📦 Batch Processing at Scale
---
The batch runner enables:
- Sequential or parallel DAG execution
- Resume from failure using offsets
- Dry-run previews

This mirrors real-world backfill and reprocessing workflows.
Example:
```bash
python batch_runner.py --total 5000000 --batch-size 5000 --parallel 5
```

---

## 📊 Output Guarantees

Every output record includes:

- **Original text** – as ingested from source  
- **Healed text** – cleaned and validated  
- **Sentiment label** – POSITIVE, NEGATIVE, or NEUTRAL  
- **Confidence score** – model certainty  
- **Healing metadata** – error type, action taken, flags  

No silent failures. No black boxes. Every step is fully observable and traceable.



---

## 🔮 Future Enhancements

Planned improvements to make the pipeline even more robust and production-ready:

- Kafka-based streaming ingestion for real-time processing  
- S3 + Snowflake warehouse integration for scalable storage and analytics  
- Data validation using Great Expectations  
- CI/CD pipelines with GitHub Actions for automated testing and deployment  
- Multi-model support for flexibility in inference
