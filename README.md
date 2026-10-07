# The Three Musketeers — Rebuilding a Real-Time Data Pipeline

> *All for one, one for all.* Three people, three backgrounds, one pipeline, rebuilt from scratch to **learn**, not just to ship.

We are rebuilding [`e2e-data-engineering`](e2e-data-engineering/) (the original tutorial project, kept in this repo as a read-only reference): a streaming pipeline that pulls fake users from the randomuser.me API, pushes them through **Kafka**, processes them with **Spark Structured Streaming** and stores them in **Cassandra**, scheduled by **Airflow** and running in **Docker**.

The original is ~230 lines of Python, and it has real bugs (as written, it sends **zero** messages; see the [appendix](#bug-hunt)). That's good news: rebuilding it properly *is* the curriculum.

**Contents**

1. [Who does what](#split)
2. [Timeline](#timeline)
3. [Shared foundations](#foundations): repo layout, contracts, versions, working agreements
4. [Week 1: all together](#week1)
5. [Athos — the Data Scientist](#athos)
6. [Porthos — the Software Engineer](#porthos)
7. [Aramis — the Data Engineer](#aramis)
8. [Week 6: rotation &amp; final demo](#week6)
9. [Reading list](#reading)
10. [Appendix: bug hunt answers](#bug-hunt)

---

<a id="split"></a>

## 1. Who does what

Codenames used in this document. Write your real names next to them:

- **Athos** = the Data Scientist → ______________
- **Porthos** = the Software Engineer → ______________
- **Aramis** = the Data Engineer → ______________

```
 ATHOS · Data Scientist                          PORTHOS · Software Engineer
 ┌──────────────────────────────┐                ┌──────────────────────────────────┐
 │ randomuser.me API            │                │ Spark Structured Streaming       │
 │   → Python producer          │     Kafka      │   → parse · validate · cast      │
 │     (fetch · clean · send)   │──────────────► │   → Cassandra (query-first model)│
 │   scheduled by Airflow DAG   │ users_created  │                                  │
 │ + data-quality checks        │                │ + dead-letter handling           │
 └──────────────────────────────┘                └──────────────────────────────────┘
 ┌──────────────────────────────────────────────────────────────────────────────────┐
 │ ARAMIS · Data Engineer: the ground everything runs on                            │
 │ Docker Compose · Kafka (KRaft) · Schema Registry · Airflow image · Spark cluster │
 │ CI · end-to-end tests · monitoring · runbook                                     │
 └──────────────────────────────────────────────────────────────────────────────────┘
```

|                                            | **Athos** (Data Scientist)                                                                           | **Porthos** (Software Engineer)                                                  | **Aramis** (Data Engineer)                                                                        |
| ------------------------------------------ | ---------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| **Wants to learn**                   | Data eng + software                                                                                        | Data eng                                                                               | Data eng (deeper) + software                                                                            |
| **Owns**                             | *Ingestion & orchestration*: API → Kafka producer, Airflow DAGs, the data contract, data-quality checks | *Stream processing & storage*: Kafka → Spark → Cassandra, the Cassandra data model | *Platform*: Docker Compose, Kafka cluster, Airflow deployment, CI/CD, end-to-end tests, observability |
| **Entry point (comfort zone)**       | Python, exploring data                                                                                     | Code structure, testing                                                                | Knows what each tool is for                                                                             |
| **Stretch zone (the real learning)** | Production Python: packages, tests, retries, logging. Orchestration, Kafka producers                       | Distributed processing, streaming semantics, NoSQL data modeling                       | Kafka internals, containers, CI/CD, test automation, engineering rigor                                  |
| **Coaches the others on**            | Data profiling, data quality, "what is this data for?"                                                     | Testing, code structure, code review, git                                              | Kafka / Spark / Airflow mental models, debugging the stack                                              |

### Why this split

- **Split by growth, not by strength.** If the Data Engineer built Kafka + Spark, the Software Engineer did Docker + CI, and the Data Scientist made a dashboard, you'd finish fast and learn almost nothing new. Each slice above sits *just outside* its owner's comfort zone, with a teammate nearby who can coach it.
- **The pipeline has two natural seams:** the Kafka topic and the Docker network. Agree on those two interfaces in week 1 ([§3.2](#contracts)) and all three of you can work in parallel without waiting for each other.
- **Everyone teaches one thing and learns two.** Porthos is the *software coach*, Aramis the *data-engineering coach*, Athos the *data coach*.

> **Rule of the musketeers:** when pairing, *the learner types, the expert talks.* The coach never grabs the keyboard. Explain, point, ask questions.

---

<a id="timeline"></a>

## 2. Timeline

Assumes ~6–8 hours per person per week. Less time? Stretch it to 8–10 weeks. Skip stretch goals, never skip tests.

| Week | Phase                   | Athos (DS)                                                                                   | Porthos (SWE)                                                 | Aramis (DE)                                                         |
| ---- | ----------------------- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------- |
| 1    | **Understand**    | *All together:* set up, make the original run, bug hunt, agree on contracts ([§4](#week1)) | ←                                                            | ←                                                                  |
| 2    | **Build**         | Profile the API, write the contract,`transform.py` + tests                                 | Spark batch basics, read Kafka in batch, Cassandra data model | Repo skeleton + CI, Compose rewrite (Kafka KRaft, Cassandra, Spark) |
| 3    | **Build**         | API client, Kafka producer, Airflow DAG                                                      | Streaming job → console → Cassandra, unit tests             | Airflow image, init jobs, Kafka lab                                 |
| 4    | **Integrate**     | End-to-end run with Porthos, fix contract gaps                                               | Run the job on the Spark cluster                              | End-to-end test + CI job                                            |
| 5    | **Harden**        | Logging, dead-letter queue, data-quality DAG                                                 | Bad-record routing, crash/restart experiment, metrics         | Schema Registry + Avro, monitoring, runbook                         |
| 6    | **Rotate & demo** | Swap: Spark windowed aggregation                                                             | Swap: DLQ replay DAG                                          | Swap: FastAPI serving layer                                         |

**Milestones:** end of W1, the original runs and the contracts are merged · end of W3, each slice works on its own · end of W4, `make up` → data lands in Cassandra · end of W6, the demo.

---

<a id="foundations"></a>

## 3. Shared foundations

### 3.1 Target repo layout

```
three-musketeers/
├── README.md                     ← this file
├── Makefile                      ← make up / down / test / e2e / reset          (Aramis)
├── docker-compose.yml                                                         (Aramis)
├── .env.example                  ← every host, port and setting lives here    (Aramis)
├── .gitattributes                ← forces LF on *.sh (Windows!)               (Aramis)
├── pyproject.toml                ← ruff + pytest config                       (Aramis)
├── .github/workflows/ci.yml                                                   (Aramis)
├── contracts/
│   └── user_created.v1.schema.json   ← Athos writes, all three approve
├── ingestion/                                                                 (Athos)
│   ├── src/ingestion/            api_client.py · transform.py · producer.py · config.py · run.py
│   ├── dags/                     user_ingestion.py · data_quality.py
│   ├── notebooks/                01_profile_randomuser.ipynb
│   └── tests/
├── processing/                                                                (Porthos)
│   ├── src/processing/           schema.py · transform.py · sinks.py · job.py
│   ├── Dockerfile
│   └── tests/
├── infra/                                                                     (Aramis)
│   ├── airflow/Dockerfile
│   ├── cassandra/init.cql        ← Porthos designs it, Aramis wires it in
│   └── kafka/create-topics.sh
├── tests/integration/test_e2e.py ← Aramis, with everyone
├── docs/
│   ├── adr/                      ← architecture decision records
│   ├── learning-log/             athos.md · porthos.md · aramis.md
│   └── runbook.md
└── e2e-data-engineering/         ← the original, read-only reference
```

<a id="contracts"></a>

### 3.2 The two contracts (agree on these in week 1)

These are the only things the three workstreams share. Once they're merged, nobody waits for anybody.

**Data contract: Kafka topic `users_created`** (owner: Athos · approvers: Porthos, Aramis)

Message key: the user `id`. Message value: JSON (moving to Avro in week 5).

| Field                              | Type                        | Notes                                                                          |
| ---------------------------------- | --------------------------- | ------------------------------------------------------------------------------ |
| `id`                             | string (UUID v4)            | Generated by the producer; also the Kafka message key                          |
| `first_name`, `last_name`      | string                      |                                                                                |
| `gender`                         | string                      |                                                                                |
| `email`, `username`, `phone` | string                      | PII                                                                            |
| `address`                        | string                      | street number + name, city, state, country                                     |
| `country`                        | string                      | *Proposed:* its own field, so it can be queried                              |
| `post_code`                      | string                      | The API returns a**number** for some countries, so always cast to string |
| `dob`                            | string (ISO-8601 timestamp) |                                                                                |
| `registered_date`                | string (ISO-8601 timestamp) |                                                                                |
| `picture`                        | string (URL)                |                                                                                |
| `ingested_at`                    | string (ISO-8601 timestamp) | *Proposed:* when the producer fetched it, so you can measure latency         |
| `schema_version`                 | int                         | *Proposed:* starts at `1`                                                  |

Open questions for your contract meeting:

- Keep `address` as one string, or split it into `city` / `state` / `country`? (Porthos's data model will want `country` as a real column.)
- Emails, phones and birth dates are PII. They're fake here, but decide now: who may see them, and may they appear in logs?
- What happens to a record that breaks the contract: dropped, or sent to a `users_dlq` topic?

**Infra contract: where things live** (owner: Aramis)

| Service                          | From another container          | From your laptop          | UI / shell                          |
| -------------------------------- | ------------------------------- | ------------------------- | ----------------------------------- |
| Kafka                            | `kafka:29092`                 | `localhost:9092`        | Kafka UI → http://localhost:8085   |
| Schema Registry                  | `http://schema-registry:8081` | `http://localhost:8081` |                                     |
| Cassandra                        | `cassandra:9042`              | `localhost:9042`        | `docker exec -it cassandra cqlsh` |
| Spark master                     | `spark://spark-master:7077`   |                           | http://localhost:9090               |
| Airflow                          |                                 |                           | http://localhost:8080               |
| Postgres (Airflow metadata only) | `postgres:5432`               | not exposed               |                                     |

Rule: **no hosts or ports hard-coded in code.** Read them from environment variables (`KAFKA_BOOTSTRAP_SERVERS`, `CASSANDRA_HOST`, …) declared in `.env.example`.

### 3.3 Stack: original vs rebuild

Aramis pins exact versions in **ADR-001** during week 1.

| Layer         | Original                                             | Rebuild                                                                | Why change                                                                                                                                                                     |
| ------------- | ---------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Orchestration | Airflow 2.6 image, pip-upgraded to 2.7 on every boot | **Airflow 3.x**, custom image built once                         | Current major version.`schedule_interval` and `airflow db init` no longer exist in 3.x                                                                                     |
| Broker        | Confluent 7.4 + ZooKeeper                            | **Apache Kafka 4.x in KRaft mode** (`apache/kafka` image)      | ZooKeeper was removed in Kafka 4.0. KRaft is how Kafka runs now                                                                                                                |
| Kafka UI      | Confluent Control Center                             | **kafbat/kafka-ui** (or Redpanda Console)                        | Open source, a fraction of the RAM                                                                                                                                             |
| Processing    | `bitnami/spark:latest` + Scala 2.13 connectors     | **Spark 3.5.x** (`apache/spark` image) + Scala 2.12 connectors | Pin versions, and the Scala suffix must match the runtime. Check the Cassandra connector's compatibility table before trying Spark 4; the connector lags behind Spark releases |
| Storage       | `cassandra:latest`                                 | **Cassandra 5.0.x**, pinned                                      | Never use`latest`                                                                                                                                                            |
| Python        | 3.9                                                  | 3.11 or 3.12                                                           | Match the Airflow image you pick                                                                                                                                               |

### 3.4 Working agreements

- **Branches:** `main` is protected. Work on `<area>/<what>`, e.g. `ingestion/api-retries`, `processing/dlq`, `infra/kraft`.
- **Pull requests:** small (aim for < 300 lines). The description says *what / why / how I tested it*. At least one approval. Reviews are for learning too: "why did you do it this way?" is a perfectly good review comment.
- **Reviewers:** the default reviewer is the *coach* for that topic. Once a week, also review one PR from the area you know least.
- **Commits:** [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `test:`, `docs:`, `chore:`).
- **Pairing:** at least one 1–2 h pairing session per person per week. The learner types.
- **Weekly sync (45 min):** demo what works → blockers → plan for next week. Then a **20-min teach-back**: one person (rotating) explains a concept they learned that week.
- **Learning log:** after each work session, add 3 bullets to `docs/learning-log/<you>.md`: *what I learned · what confused me · what I'll look up next.* This becomes your blog post and your interview material.
- **Decisions:** anything you might argue about later (versions, data model, delivery guarantees) gets a half-page ADR in `docs/adr/`: *context → decision → consequences*.
- **Stuck rule:** stuck > 45 min → post in the team chat what you tried. Being stuck isn't failure. Being stuck in silence is.
- **AI assistants:** use them to explain errors and concepts. Write your own slice's code yourself first; the struggle is where the learning happens.

**Definition of done** (every task):

- [ ] Merged through a reviewed PR, CI green
- [ ] Has tests (unit tests for logic, integration tests where it touches a real service)
- [ ] Works from a fresh clone with `make up`
- [ ] No hard-coded hosts, ports or secrets
- [ ] Docs updated, learning-log entry written

---

<a id="week1"></a>

## 4. Week 1: all together

### Setup (everyone, before the first session)

- [ ] **Work inside WSL2, not directly on Windows.** Install WSL2 + Ubuntu, Docker Desktop (WSL2 backend), and VS Code with the *WSL* extension. Clone the repo into your WSL home (e.g. `~/code/three-musketeers`), **not** into OneDrive. This avoids three classic traps: CRLF line endings breaking shell scripts inside containers (this already happened to `entrypoint.sh` in this folder), OneDrive locking and syncing files, and very slow bind mounts.
- [ ] Inside WSL: `git config --global core.autocrlf input`.
- [ ] **Give Docker enough RAM.** The full stack (Kafka, Spark, Cassandra, Airflow, Postgres) is heavy. In `%UserProfile%\.wslconfig`, set `memory=8GB` (or more) under `[wsl2]`. A laptop with ≤ 8 GB total runs the `core` profile only.
- [ ] Python 3.11+ in WSL, one virtual environment per component. Porthos (and CI) also need Java 17 for local Spark tests. Nobody installs Spark on Windows.
- [ ] Create the GitHub repo, add all three of you, protect `main`.
- [ ] The cloned original has its own `.git` folder in `e2e-data-engineering/`, so Git would treat it as an embedded repo. Delete `e2e-data-engineering/.git` (you can always re-clone it) so it's committed as plain reference files.
- [ ] Each of you: watch the original video tutorial (linked in [the original README](e2e-data-engineering/README.md)) and read all five source files.

### Session 1: make the original run (mob programming, ~3 h)

One laptop on the big screen. The driver rotates every 20 minutes, and the other two navigate.

1. `docker compose up -d` in `e2e-data-engineering/`, then `docker compose ps`. Which services are healthy? Read the logs of the ones that aren't.
2. Open Airflow (http://localhost:8080) and trigger `user_automation`. Is the task green? Now **check whether any message actually reached the topic** (Control Center at http://localhost:9021, or `kafka-console-consumer` inside the broker). *Green ≠ working.*
3. Get `spark_stream.py` running. Hint: not on Windows. Use `spark-submit` inside the `spark-master` container, or WSL.
4. Count the rows: `docker exec -it cassandra cqlsh -e "SELECT count(*) FROM spark_streams.created_users;"`
5. Trace one user by hand, from the API response all the way to its Cassandra row.

It will **not** work out of the box. That's the point: debugging an unfamiliar pipeline is the #1 data-engineering skill. Stuck for more than 30 minutes on one problem? Peek at the 🔴 items in the [appendix](#bug-hunt).

### Session 2: bug hunt & contracts (~2 h)

1. On your own (30 min): list every problem you see in the original: bugs, risks, bad smells.
2. Merge your three lists, *then* open the appendix and compare. Sort each item into "fix in rebuild" or "ignore".
3. Contract meeting: agree on the data contract and the infra contract ([§3.2](#contracts)). Athos opens the contract PR. Aramis opens the repo-skeleton PR.
4. Aramis writes ADR-001 (versions).

**Week 1 is done when:**

- [ ] Each of you can draw the architecture from memory and explain the path of one record
- [ ] The original ran end-to-end at least once (or you documented exactly why it couldn't)
- [ ] The contract PR and the repo-skeleton PR are merged

---

<a id="athos"></a>

## 5. Athos — the Data Scientist → owns *Ingestion & Orchestration*

**Why this slice is yours.** You already speak Python and data, so the entry barrier is low. The real goal is the jump from notebook code to code that runs unattended at 3 a.m.: modules instead of cells, tests instead of eyeballing, retries and logs instead of "just re-run it". That jump *is* the move from data science to software and data engineering. You also own the data contract and data quality, where your instincts are an advantage the other two don't have.

**What you'll learn**

- *Software:* Python packaging (src layout, `pyproject.toml`), single-purpose functions, type hints, pytest with fixtures and mocks, logging, configuration through environment variables, a git + PR workflow.
- *Data engineering:* Airflow (DAGs, tasks, scheduler, retries, `catchup`, idempotency), Kafka producers (topics, keys, partitions, `acks`, batching, `flush`), data contracts and schema evolution, dead-letter queues, data-quality checks.

**Your coaches:** Porthos for software practices, Aramis for Kafka and Airflow.

📘 **Detailed guide:** [docs/guides/athos.md](docs/guides/athos.md) covers prerequisites, concept primers, step-by-step hints and common traps.

### Plan

**Week 2: from notebook to package**

- [ ] **Profile the source** in `ingestion/notebooks/01_profile_randomuser.ipynb`. Fetch ~500 users from `https://randomuser.me/api/?results=500&seed=musketeers` (the seed makes the data reproducible). Which fields change type between records? Which are nested? Any nulls, duplicates or strange values? This feeds the contract.
- [ ] **Refine the contract** `contracts/user_created.v1.schema.json` (JSON Schema) with what profiling taught you (the week-1 draft was a first guess) and get the update approved.
- [ ] **Create the package** `ingestion/src/ingestion/` with its own `pyproject.toml`. The first module is `transform.py`, holding a *pure* function `to_contract(raw: dict) -> dict`: no network, no Kafka, just data in and data out. Use type hints throughout.
- [ ] **First tests** in `ingestion/tests/test_transform.py`. Save 3–5 real API responses as JSON fixtures. Test the numeric postcode, accented names and a missing field. Validate every output against the contract with the `jsonschema` library.
- [ ] **Unblock Porthos:** commit `contracts/samples/sample_messages.jsonl` (~50 valid messages) and `bad_messages.jsonl` (~5 broken ones) as soon as `to_contract` works.

✅ *Done when* `pytest` passes locally and in CI, and the sample files are committed.

**Week 3: talk to the outside world**

- [ ] `api_client.py`: a `requests.Session` with a timeout on every call, retries with exponential backoff on 5xx errors and timeouts, and **batches** (`results=N`) instead of one call per user.
- [ ] Test it with mocked HTTP (`responses` or `unittest.mock`): a timeout, an HTTP 500, invalid JSON. Unit tests never touch the real network.
- [ ] `producer.py`: a small class wrapping the Kafka producer. It handles JSON serialization, sets `key=id` and `acks="all"`, counts successes and failures, and always calls `flush()` (use a context manager or `try/finally`).
- [ ] `config.py`: settings from environment variables (`KAFKA_BOOTSTRAP_SERVERS`, `USERS_TOPIC`, `BATCH_SIZE`) with sane defaults.
- [ ] Run it **without Airflow** first, with `python -m ingestion.run --count 50`, and watch the messages arrive in Kafka UI.
- [ ] Only then write the DAG `ingestion/dags/user_ingestion.py` in TaskFlow style. The DAG file stays thin: it calls your package and holds no logic. Replace the original "loop for 60 seconds" with *fetch N users → produce → flush → report counts*. Set `retries`, `retry_delay`, `schedule`, `catchup=False` and a `doc_md`.

✅ *Done when* triggering the DAG produces exactly N messages, **and the task turns red if Kafka is down** (no more silent green).

**Week 4: integrate**

- [ ] With Porthos: run producer → Spark → Cassandra and compare counts at every hop (API → topic → table). Where do records go missing?
- [ ] Fix any contract mismatches. If the contract has to change, do it *compatibly* (add optional fields; never rename or remove) and bump the version.

**Week 5: make it production-shaped**

- [ ] Replace every `print` with `logging`. Log counts per run, and **never log PII** (emails, phones).
- [ ] Records that fail validation go to the `users_dlq` topic with an `error` field instead of disappearing.
- [ ] **Data-quality DAG** `dags/data_quality.py`, your data-science superpower. After ingestion, query Cassandra and check rules you define: row count vs. produced count, null rate per column, duplicate emails, birth dates in the future, malformed emails. Any broken rule fails the DAG. You own the rules, and Porthos reviews the code.

✅ *Done when* you can show a broken record landing in the DLQ, and a bad row (inserted by hand) making the quality DAG fail.

### Stretch goals

- Avro + Schema Registry on the producer side (with Aramis, week 5).
- Benchmark `kafka-python` vs. `confluent-kafka` (messages/sec, with a chart): a data-science experiment on a data-engineering question.
- A small Streamlit dashboard on Cassandra for the final demo (signups per country, age distribution).

### Self-check: can you answer these?

1. What do `start_date`, `schedule` and `catchup` each control? What would `catchup=True` with a 2023 start date do?
2. What makes a task *idempotent*? If your DAG runs twice for the same day, what happens downstream?
3. Why does the original import `requests` and `kafka` *inside* the functions instead of at the top of the DAG file?
4. What does `producer.flush()` do, and what can be lost without it?
5. What is the message key for? What would change if you keyed by `country` instead of `id`?
6. What do you trade between `acks=0`, `acks=1` and `acks=all`?
7. Why do unit tests mock the API? What *does* need a real call?
8. Why was the original task green while it sent zero messages, and how does your version prevent that?

### Study material

- Airflow docs: [Core Concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html) and [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) (idempotency, top-level code)
- Kafka docs: [Producer configs](https://kafka.apache.org/documentation/#producerconfigs) (`acks`, `linger.ms`, `batch.size`, `enable.idempotence`)
- [Python Packaging User Guide](https://packaging.python.org/): the *src layout*
- [pytest docs](https://docs.pytest.org/) and *Python Testing with pytest* (Brian Okken)
- *Fundamentals of Data Engineering* (Reis & Housley): the ingestion and orchestration chapters

---

<a id="porthos"></a>

## 6. Porthos — the Software Engineer → owns *Stream Processing & Storage*

**Why this slice is yours.** It's the part least like normal backend work. You're not handling requests: you're running a query over an *unbounded table* that never finishes. Micro-batches, offsets, checkpoints, delivery guarantees and query-first data modeling are all new territory, and this is exactly where data engineering differs from software engineering. Your habits (clean structure, tests) will also make this the best-engineered component.

**What you'll learn**

- *Data engineering:* Spark architecture (driver, executors, master/worker, lazy evaluation, query plans), Structured Streaming (sources, sinks, triggers, output modes, checkpoints, `foreachBatch`, watermarks), the consumer side of Kafka (offsets, partitions → tasks), Cassandra modeling (partition vs. clustering keys, one table per query, upserts), delivery semantics (at-least-once + idempotent writes).
- *Software (you teach this one):* making data code testable, with pure transform functions and a local `SparkSession` in pytest.

**Your coach:** Aramis for Spark, Kafka and Cassandra. You coach the other two on software.

### Plan

**Week 2: think in DataFrames**

- [ ] Get PySpark running in WSL (`pip install "pyspark==3.5.*"` + Java 17). Never directly on Windows.
- [ ] **Batch first.** Ask Athos for `sample_messages.jsonl` (contract-valid messages). Read it with `spark.read.json`, select columns, cast `dob` and `registered_date` to timestamps, and `.explain()` the plan. In your learning log, write down the difference between transformations and actions, and why nothing runs until `.show()`.
- [ ] **Kafka, still in batch:** run `spark.read.format("kafka")` on the topic and look at the raw columns (`key`, `value`, `topic`, `partition`, `offset`, `timestamp`). Kafka is just an ordered log of bytes, and *you* give it a schema.
- [ ] **Data model, query-first.** Write the queries before the tables, e.g. Q1 "get a user by id", Q2 "latest users who registered in country X", Q3 "signups per country per day". Design one table per query (`users_by_id`, `users_by_country`, …), choose partition and clustering keys, and ask yourself *could one partition grow forever?* Put it in `infra/cassandra/init.cql` and write an ADR.

✅ *Done when* you can explain why the original table only supports "get user by id".

**Week 3: go streaming**

- [ ] Package `processing/src/processing/`: `schema.py` (the Spark schema built from the contract), `transform.py` (pure DataFrame → DataFrame functions), `sinks.py`, and `job.py` (wiring + config from env).
- [ ] Add a test that fails if `schema.py` and the JSON contract drift apart.
- [ ] Streaming v1 → **console sink**. Experiments to write up: change the trigger interval; compare `startingOffsets` earliest vs. latest; stop and restart *with* and *without* a checkpoint. What gets reprocessed?
- [ ] Streaming v2 → **Cassandra** via `foreachBatch`, writing to every table in your model.
- [ ] Unit tests with a session-scoped local `SparkSession` fixture: parsing, invalid JSON → null, timestamp casting, country extraction.

✅ *Done when* messages typed into `kafka-console-producer` show up in all your Cassandra tables, and the tests pass in CI.

**Week 4: run on the real cluster**

- [ ] With Aramis: write `processing/Dockerfile` and a `spark-submit --master spark://spark-master:7077 --packages …` service in Compose, using in-network hostnames (`kafka:29092`, `cassandra`).
- [ ] Open the Spark UI (http://localhost:9090) and prove the job's executors run on the worker. (The original never used its own cluster. Work out why.)
- [ ] End-to-end run with Athos's DAG.

**Week 5: correctness**

- [ ] Bad records (unparseable JSON, missing required fields) → the `users_dlq` topic or a `users_rejected` table, together with the reason. Nothing is silently dropped.
- [ ] **Crash experiment:** `docker kill` the job mid-stream, restart it and count the rows. Explain the result: Cassandra writes are upserts on the primary key, so replays overwrite instead of duplicating. That's *at-least-once + an idempotent sink*. Write the ADR.
- [ ] Keep checkpoints on a named Docker volume, not `/tmp`.
- [ ] Add `processed_at`. Log per-batch stats from `query.lastProgress` (rows/sec, batch duration), and compute end-to-end latency from `ingested_at`.

✅ *Done when* you can kill and restart the job without losing or duplicating a row, and explain why.

### Stretch goals

- Windowed aggregation with a watermark: signups per country per minute → `signups_by_country_minute`. Careful: counters are *not* idempotent, so what does the crash experiment do now?
- Also write the raw stream to Parquet partitioned by date (a "bronze" layer). Compare a data lake with a serving database.
- A one-page comparison: Spark Structured Streaming vs. Flink vs. Kafka Streams.

### Self-check: can you answer these?

1. Transformations vs. actions: when does Spark actually do work?
2. In a streaming query, what are a micro-batch, a trigger and an output mode?
3. What exactly lives in the checkpoint folder? What happens if you delete it and restart with `startingOffsets=earliest`? With `latest`?
4. Is the pipeline at-least-once or exactly-once end to end? Why are duplicates harmless with your tables, and when would they stop being harmless?
5. Partition key vs. clustering key: why can't you `WHERE country = 'France'` on the original table?
6. Driver vs. executor: where does your Python code run? What did the Spark UI show in local mode vs. on the cluster?
7. What is a watermark, and why does a windowed aggregation need one?
8. Why does the Scala suffix (`_2.12` / `_2.13`) on a Spark package matter?

### Study material

- [Structured Streaming Programming Guide (Spark 3.5)](https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html)
- *Learning Spark*, 2nd ed. (Damji et al.): the DataFrame and Structured Streaming chapters
- [Cassandra docs](https://cassandra.apache.org/doc/latest/): the *Data Modeling* section
- *Designing Data-Intensive Applications*: ch. 3 (LSM-trees, i.e. why Cassandra writes are fast), ch. 6 (partitioning), ch. 11 (stream processing)

---

<a id="aramis"></a>

## 7. Aramis — the Data Engineer → owns *Platform, Kafka & Quality Gates*

**Why this slice is yours.** You already know what each tool is *for*. The next level has two parts. First, how the tools really run: Kafka internals, networking, resources. Second, the software discipline that turns a "works on my machine" demo into something anyone can clone and run: pinned versions, built images, CI, automated tests, runbooks. That's what separates a junior data engineer from a senior one. You also unblock the team in week 1, so you move first.

**What you'll learn**

- *Data engineering, deeper:* Kafka internals (partitions, replication, ISR, retention, listeners), KRaft vs. ZooKeeper, Schema Registry and compatibility modes, Airflow architecture (api-server, scheduler, dag-processor, executor, metadata DB), Spark cluster resources.
- *Software:* Docker (images vs. containers, layers, healthchecks, volumes, networks), Compose profiles, Makefiles, GitHub Actions, pre-commit and linting, test automation, secrets handling, documentation.

**Your coach:** Porthos for software practices (CI, test design, code review). You coach the other two on Kafka, Spark and Airflow.

### Plan

**Week 1: unblock everyone (do this first)**

- [ ] Repo skeleton ([§3.1](#foundations)) with `.gitattributes` (`*.sh text eol=lf`), `.editorconfig`, `.gitignore` and `.env.example`.
- [ ] A root `pyproject.toml` with ruff + pytest config, `pre-commit` hooks and a PR template.
- [ ] A `Makefile` with `make up`, `make down`, `make logs`, `make test`, `make e2e` and `make reset`.
- [ ] CI v1 (GitHub Actions): ruff + unit tests for each component on every PR. (The Spark tests need Java, so use `actions/setup-java`.)
- [ ] ADR-001: versions and images, all pinned.

**Week 2: Compose, rewritten line by line**

- [ ] Write `docker-compose.yml` from scratch, one service at a time, and understand every line: Kafka in KRaft mode (no ZooKeeper), Cassandra, Spark master + worker. Pin everything, give every service a real healthcheck, use `depends_on: condition: service_healthy`, named volumes and memory limits.
- [ ] Init jobs: `kafka-init` creates `users_created` (3 partitions) and `users_dlq`; `cassandra-init` applies `init.cql` once Cassandra is healthy.
- [ ] Compose **profiles**, so 8 GB laptops survive: `core` (Kafka, Cassandra, Spark), `orchestration` (Airflow + Postgres), `tools` (Kafka UI, Schema Registry).
- [ ] Publish the infra contract ([§3.2](#contracts)) and `.env.example`.

✅ *Done when* a fresh clone + `make up` gives an all-healthy `core` stack, and `make reset` wipes everything.

**Week 3: the Airflow platform + a Kafka deep-dive**

- [ ] Custom Airflow image `infra/airflow/Dockerfile`: the official image plus only the deps you need and Athos's `ingestion` package, all installed at **build** time. No pip at startup, no 135-line freeze. Use a Postgres metadata DB and `airflow db migrate`, create the admin user from env vars, and disable example DAGs *the right way*. Airflow 3 runs as several components (api-server, scheduler, dag-processor, triggerer). Run them as separate services and be able to explain what each one does.
- [ ] Kafka lab. Write each item up in your learning log:
  - listeners: connect from a container and from your laptop, then break the advertised listeners on purpose and read the error;
  - keys and partitions: does the same key always land on the same partition? Prove it;
  - consumer groups and lag: run `kafka-consumer-groups.sh --describe` while Spark is running;
  - retention: what happens to old messages?
- [ ] Add Kafka UI and compare its RAM use to Control Center's with `docker stats`.

**Week 4: prove it works, automatically**

- [ ] `tests/integration/test_e2e.py`: against a running `core` stack, produce N contract-valid messages (reuse Athos's producer), poll Cassandra until N rows appear or a timeout hits, then assert. Run it with `make e2e`.
- [ ] CI v2: a job that starts the `core` profile in GitHub Actions and runs `make e2e`, on PRs to `main` (or nightly, if it's too slow).
- [ ] Pair with Porthos on running the Spark job on the cluster.

**Week 5: contracts, visibility, operations**

- [ ] **Schema Registry for real** (with Athos and Porthos): turn the JSON contract into an Avro schema, set compatibility to `BACKWARD`, then demo a breaking change being rejected and a compatible one being accepted. Heads-up: the Confluent wire format adds a 5-byte header that Spark's `from_avro` doesn't expect. Solving that is part of the exercise.
- [ ] Observability: know where to read consumer lag, messages/sec and Spark batch duration (Kafka UI, Spark UI, Airflow UI). Stretch: Prometheus + Grafana.
- [ ] Secrets: nothing secret in `docker-compose.yml`, `.env` is git-ignored, and the Airflow secret key is new.
- [ ] `docs/runbook.md`: how to start, stop and reset, plus the top failures and their fixes (Cassandra slow to start, listener errors, CRLF scripts, out-of-memory).

✅ *Done when* a teammate can go from a fresh clone to data in Cassandra using only the README and the runbook.

### Stretch goals

- 3 Kafka brokers with replication factor 3: kill one and watch leader election and the ISR shrink.
- Build images and push them to GitHub Container Registry from CI; tag releases with a changelog.
- Run 2 Cassandra nodes and explore replication factor vs. consistency level.

### Self-check: can you answer these?

1. Explain advertised listeners to a teammate: why does a container use `kafka:29092` while your laptop uses `localhost:9092`?
2. What did ZooKeeper do for Kafka, and what replaced it in KRaft mode?
3. Partitions, replication factor, ISR: what happens when a broker dies with RF=1? With RF=3?
4. `depends_on` vs. `depends_on: condition: service_healthy`: what's the difference, and what should a good healthcheck actually test?
5. Image vs. container: why is `pip install` at container start a bad idea?
6. What does Airflow keep in its metadata DB, and why isn't SQLite good enough beyond a demo?
7. What is consumer lag, how do you measure it, and when should it wake someone up?
8. `BACKWARD` vs. `FORWARD` compatibility: with each one, who must upgrade first, the producer or the consumer?

### Study material

- [Kafka docs](https://kafka.apache.org/documentation/): the *Design* and *KRaft* sections
- *Kafka: The Definitive Guide*, 2nd ed. (Shapira et al.)
- [Docker Compose docs](https://docs.docker.com/compose/): file reference, healthchecks, profiles
- Airflow docs: *Running Airflow in Docker*. Their compose file is a good model to compare yours against
- [GitHub Actions docs](https://docs.github.com/actions)
- *The Pragmatic Programmer* (Hunt & Thomas): the software mindset, in short chapters

---

<a id="week6"></a>

## 8. Week 6: rotation & final demo

### Swap tasks

Each person does one task in an area they didn't own, so nobody ends the project knowing only a third of it.

| Who                     | Swap task                                                                                                                                                 | Coach                                 |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- |
| **Athos** (DS)    | **Spark:** windowed aggregation "signups per country per minute" with a watermark → a new Cassandra table, with tests                              | Aramis (Porthos reviews as the owner) |
| **Porthos** (SWE) | **Airflow + Kafka:** a DLQ replay DAG that reads `users_dlq`, re-validates the records and re-publishes the fixable ones to `users_created`     | Athos                                 |
| **Aramis** (DE)   | **Software:** a small FastAPI service over Cassandra (`GET /users/{id}`, `GET /stats/countries`) with tests, a Dockerfile and a Compose service | Porthos                               |

### Final demo (~1 h; invite friends or colleagues)

1. **Fresh-clone test:** on the laptop of someone who didn't set it up, run `git clone` → `make up` → trigger the DAG.
2. Follow one record live: Airflow log → Kafka UI → Spark UI → Cassandra / the API.
3. **Teach-back:** each person spends 10 minutes presenting a component they did *not* own. Athos presents the platform, Porthos the ingestion, Aramis the processing. If you can't explain it, you haven't learned it yet.
4. Retro: what was hardest, and what would you do differently? Update this README with the answers.
5. Optional: each of you writes a short post about what you learned. Your learning log is the first draft.

---

<a id="reading"></a>

## 9. Reading list (shared)

- ***Designing Data-Intensive Applications*** (Martin Kleppmann): the team book. Read one chapter every 1–2 weeks and discuss it in the weekly sync. Priority: ch. 3, 5, 6, 11.
- ***Fundamentals of Data Engineering*** (Joe Reis & Matt Housley): a map of the whole field. Athos and Porthos should read it first.
- The original video tutorial (linked in [the original README](e2e-data-engineering/README.md)).
- Per-person material is listed at the end of each person's section.

---

<a id="bug-hunt"></a>

## 10. Appendix: bug hunt answers

<details>
<summary><b>Spoilers: do the bug hunt yourselves first (week 1, session 2)</b></summary>

**🔴 Stops the original from working**

1. [`script/entrypoint.sh`](e2e-data-engineering/script/entrypoint.sh): when cloned on Windows with `core.autocrlf=true`, the file gets CRLF line endings, and the Linux container fails with errors like `/bin/bash^M: bad interpreter`. Fix: LF endings, enforced through `.gitattributes`.
2. [`dags/kafka_stream.py#L23`](e2e-data-engineering/dags/kafka_stream.py#L23): `uuid.uuid4()` is not JSON-serializable, so `json.dumps` at [L55](e2e-data-engineering/dags/kafka_stream.py#L55) raises on **every** record. The `except` at [L56-58](e2e-data-engineering/dags/kafka_stream.py#L56) logs it and continues, so the task runs for 60 s, sends **zero** messages, and is marked **success**. Lesson: never swallow exceptions in a loop.
3. [`docker-compose.yml#L168`](e2e-data-engineering/docker-compose.yml#L168): `bitnami/spark:latest`. Bitnami restructured its free Docker Hub catalog in 2025, so this image may fail to pull or change under you. Pin versions, and prefer the official `apache/spark` image.
4. [`spark_stream.py#L72-73`](e2e-data-engineering/spark_stream.py#L72): the connector packages are built for Scala **2.13**, but pip's `pyspark` 3.x ships Scala **2.12**, which typically ends in class-loading errors. Also, running it on the Windows host needs Java + Hadoop binaries; run it in a container or WSL instead.

**🟡 Works, but wrong or misleading**

5. [`spark_stream.py#L70-75`](e2e-data-engineering/spark_stream.py#L70): no `.master(...)` and `localhost` hosts mean the job runs in *local mode* on the host. The Spark master and worker in Compose sit idle the whole time.
6. [`spark_stream.py#L37-63`](e2e-data-engineering/spark_stream.py#L37): `insert_data()` is never called, and it inserts a `dob` column that doesn't exist in the table ([L18-32](e2e-data-engineering/spark_stream.py#L18)). It's dead code that would fail if anyone called it.
7. [`spark_stream.py#L115-127`](e2e-data-engineering/spark_stream.py#L115): the schema has no `dob`, so the producer sends it and Spark silently drops it.
8. [`spark_stream.py#L66-111`](e2e-data-engineering/spark_stream.py#L66): "log the error and return `None`". The real error is hidden and the crash happens later, somewhere else (e.g. `None.selectExpr`).
9. [`spark_stream.py#L153`](e2e-data-engineering/spark_stream.py#L153): the checkpoint lives in `/tmp`, so it's lost on restart. Combined with `startingOffsets=earliest`, the whole topic gets replayed (harmless only because Cassandra upserts).
10. [`spark_stream.py#L131`](e2e-data-engineering/spark_stream.py#L131): `print(sel)` prints the DataFrame object, not its data. A leftover debug line.
11. [`kafka_stream.py#L48-58`](e2e-data-engineering/dags/kafka_stream.py#L48): a tight loop that hammers a free public API for 60 s with no pause and no batching, and `producer.flush()` is never called, so the last messages can be lost.
12. [`kafka_stream.py#L29`](e2e-data-engineering/dags/kafka_stream.py#L29): `post_code` is a number for some countries and a string for others.
13. [`kafka_stream.py#L62`](e2e-data-engineering/dags/kafka_stream.py#L62): `schedule_interval` is deprecated and gone in Airflow 3 (use `schedule`).
14. [`docker-compose.yml#L114-115`](e2e-data-engineering/docker-compose.yml#L114): `LOAD_EX` and `EXECUTOR` come from the old community `puckel/docker-airflow` image. The official image ignores them, so the ~50 example DAGs *are* loaded. The real settings are `AIRFLOW__CORE__LOAD_EXAMPLES` and `AIRFLOW__CORE__EXECUTOR`.
15. [`docker-compose.yml#L108`](e2e-data-engineering/docker-compose.yml#L108) vs. [`requirements.txt#L5`](e2e-data-engineering/requirements.txt#L5): the image is Airflow 2.6.0 but the requirements pin 2.7.0, so pip upgrades Airflow inside the container on every start.
16. [`requirements.txt`](e2e-data-engineering/requirements.txt): a 135-line `pip freeze` for a project that needs about three direct dependencies, installed at container start ([`entrypoint.sh#L4-7`](e2e-data-engineering/script/entrypoint.sh#L4), [`docker-compose.yml#L150`](e2e-data-engineering/docker-compose.yml#L150)). The result: slow, fragile boots.
17. [`entrypoint.sh#L9`](e2e-data-engineering/script/entrypoint.sh#L9): it checks for the SQLite file `airflow.db`, but the database is Postgres. The condition is always true, so `db init` + user creation run on every boot.
18. [`entrypoint.sh#L5`](e2e-data-engineering/script/entrypoint.sh#L5): `$(command python) pip install …` works by accident (it runs `python` on an empty stdin, which prints nothing).
19. [`docker-compose.yml#L117`](e2e-data-engineering/docker-compose.yml#L117) + [`entrypoint.sh#L11-17`](e2e-data-engineering/script/entrypoint.sh#L11): a hard-coded secret key and `admin` / `admin`.
20. [`docker-compose.yml#L189`](e2e-data-engineering/docker-compose.yml#L189): `cassandra:latest`, unpinned.
21. Schema Registry and Control Center run, but nothing uses them (the messages are schemaless JSON), and Control Center is one of the hungriest services in the stack.
22. The original README says Airflow stores the data in PostgreSQL and Kafka streams it from there. In the code, Postgres is only Airflow's own metadata database.

**🔵 Missing**

23. No tests, no CI, no linting. `pyspark` is commented out ([`requirements.txt#L101`](e2e-data-engineering/requirements.txt#L101)), so the Spark job's dependencies aren't declared anywhere.
24. The Cassandra table is keyed only by `id`, so the only query it can serve is "get user by id".
25. No dead-letter handling, no metrics, no data-quality checks, no runbook.

</details>
