# Query Risk Scoring – Proof of Concept

This is a small but functional prototype for estimating the execution-risk of SQL queries before they hit the engine. The goal is simple: detect expensive patterns early, reduce cluster blow-ups, and give the planner enough signal to size resources intelligently.

Everything here is local, fast, and self-contained so it’s easy to review.

---

## What this does

* Parses an incoming SQL query
* Extracts structural features (joins, nesting, SELECT *, window functions, table sizes, etc.)
* Applies basic heuristics to catch catastrophic patterns outright (Cartesian joins, huge SELECT *)
* Runs an XGBoost classifier trained on synthetic labeled queries
* Returns:

  * risk class (low / medium / high)
  * probabilities
  * extracted metadata
  * SHAP explanation for model-driven decisions

The entire scoring pipeline runs in a few milliseconds.

---

## Why this is useful

This is the kind of lightweight probe that can sit between a query gateway and an execution planner.
It gives the engine a clear win:

* Detect unbounded fan-out before it hits compute
* Catch SELECT * on big fact tables
* Spot deep nesting and window-heavy workloads
* Produce a consistent risk score the planner can act on
* Provide explainability so the decision isn’t a black box
* Easy to retrain as more telemetry comes in

It’s a clean way to reduce waste and protect shared clusters without slowing anything down.

---

## Risk labeling (how the synthetic dataset was built)

The model is trained on synthetic queries, but the labels follow simple objective rules so the classifier learns patterns that actually map to engine pressure.

**Low Risk**

* small tables
* ≤1 join
* no heavy operations
* predictable cardinality

**Medium Risk**

* mid-sized tables
* 2–3 joins
* window functions
* moderate subquery depth
* some uncertainty in row explosion

**High Risk**

* Cartesian joins
* huge tables
* SELECT * on large fact tables
* deep nesting
* multi-join chains with poor filters
* patterns that cause shuffle/scan spikes

Hard overrides are applied even before the model runs, so catastrophic queries never sneak through.

---

## Model performance (quick view)

Based on the latest synthetic dataset:

* accuracy: ~0.89
* low false-negatives (high-risk marked low): eliminated through rule overrides
* false-positives: acceptable for a PoC
* multiclass separation is stable
* SHAP values consistently reflect the real contributors (joins, table sizes, star-selects)

Good enough to demonstrate the concept. Easy to improve once real logs are available.

---

## How to run

Install deps:

```
pip install -r requirements.txt
```

Train:

```
python train_model.py
```

Single query inference:

```
python infer.py "SELECT * FROM big_sales_table WHERE amount > 500"
```

Batch test (runs all 60 curated SQL cases):

```
python batch_infer.py
```

Streamlit UI:

```
streamlit run app_streamlit.py
```

---

## Output structure

```
{
  prediction: 0/1/2,
  probabilities: [...],
  metadata: {...},
  shap: {...}
}
```

Metadata includes table counts, join counts, estimated table size, join output estimates, flags for SELECT *, Cartesian joins, windows, filters, etc.

SHAP explains what pushed the score in that direction.

---

## Architecture (scoring pipeline)

```md
%%{init: {'theme':'neutral'}}%%
flowchart LR

A[User submits SQL] --> B[Query sent to Risk Service]

B --> C[Metadata Extractor<br/>Parse SQL → numeric features]
C --> D[Model<br/>Predict low / med / high]

D --> E[SHAP<br/>Feature contributions (model-only)]
C -->|Hard rule trigger| F[Override → High Risk]

E --> G[Build JSON Response]
F --> G
D --> G
C --> G

G --> H[Return risk score + metadata + explanations]
```

---

## Where this fits in a real system

```md
%%{init: {'theme':'dark'}}%%
graph TB
    User[User SQL UI / API]
    Gateway[Query Control Plane / Gateway]
    Risk[Query-Risk Scoring Probe]
    
    Planner[Execution Planner]
    Engine[e6data Engine]
    Storage[Object Storage]

    Telemetry[Telemetry + Logging]
    Observability[Observability Pipeline]
    Datastore[Training Data Store]

    User -->|Submit SQL| Gateway
    Gateway -->|Pre-exec check| Risk

    Risk -->|High risk → warn/block| Gateway
    Risk -->|Safe → allow| Planner

    Planner --> Engine
    Engine --> Storage

    Planner --> Telemetry
    Telemetry --> Observability
    Observability --> Datastore
```

