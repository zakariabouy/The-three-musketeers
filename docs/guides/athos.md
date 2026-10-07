# Athos's Field Guide: Ingestion & Orchestration

> You are the front door of the pipeline. Every record that ever reaches Cassandra passed through your code first.

This guide adds detail to your section of the [team README](../../README.md#athos). The README tells you **what** to build each week. This guide tells you **what to know first, how to approach each step, and where the traps are.**

---

## 0. Your role in one picture

```
 randomuser.me ──► api_client ──► transform ──► producer ──► Kafka topic `users_created` ──► Porthos's Spark job
                    (fetch)       (clean to      (send)                                            │
                                   contract)                                                       ▼
 └──────── Airflow DAG: schedules it, retries it, turns red when it fails ────────┘           Cassandra
                                                                                                   │
            data_quality DAG ◄──────────────────────── reads & checks ────────────────────────────┘
```

You have three jobs:

1. **Produce correct data:** every message matches the contract.
2. **Define what "correct" means:** the contract and the data-quality rules. This is where your data-science background is an advantage.
3. **Make it run alone, reliably:** no notebook, no human re-running cells, and loud failures.

---

## 1. Before you start: check yourself

You don't need to master all of this before week 2, but each row you answer "no" to will slow you down. Fix the gaps in week 1.

| Skill                             | Can you…                                                                                                | If not, learn it (approx. time)                                                           |
| --------------------------------- | -------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| **Python beyond notebooks** | split code into 2`.py` files, import one from the other, and run it with `python -m package.module`? | Real Python:*Python Modules and Packages* (2 h)                                         |
| **Virtual environments**    | create a venv, activate it,`pip install`, and explain what `pip install -e .` does?                  | Python Packaging User Guide:*Installing packages* (30 min)                              |
| **Git**                     | create a branch, commit, push, open a PR, update your branch from`main`, and resolve a conflict?       | [learngitbranching.js.org](https://learngitbranching.js.org/) + *Pro Git* ch. 2–3 (3 h) |
| **Linux shell (WSL)**       | use`cd`, `ls`, `cat`, `grep`, pipes, and set an env var with `export X=1`?                     | any "bash basics" tutorial (1 h)                                                          |
| **Docker**                  | explain image vs. container, and use`docker compose up -d / ps / logs -f / exec / down -v`?            | Docker docs:*Get started* (2 h)                                                         |
| **HTTP & JSON**             | explain status codes 200 / 429 / 500, what a timeout is, and which Python types JSON*can't* hold?      | MDN*HTTP overview* (1 h)                                                                |
| **pytest**                  | write a test, run it, and use`@pytest.mark.parametrize`?                                               | pytest docs:*Get started* (1 h)                                                         |

**Not needed yet:** Spark, Cassandra, Java, Scala, Kubernetes, cloud. Don't install or study them now; that's someone else's slice until week 6.

---

## 2. Five concepts to understand before writing any code

### 2.1 Kafka, from the producer's side

A good analogy for a data scientist is **an append-only CSV file that never closes, split into shards.**

| Kafka word          | Meaning                                                                                       | Analogy        |
| ------------------- | --------------------------------------------------------------------------------------------- | -------------- |
| **Topic**     | a named stream of messages (`users_created`)                                                | a table name   |
| **Partition** | a shard of the topic; order is guaranteed only*within* one partition                        | one CSV shard  |
| **Offset**    | the position of a message inside a partition                                                  | a row number   |
| **Key**       | decides the partition:`hash(key) % partitions`. Same key → same partition → kept in order | a group-by key |
| **Producer**  | your code that writes messages                                                                |                |
| **Consumer**  | Porthos's Spark job, which reads them and remembers its offset                                |                |

Two facts that bite every beginner:

- **`send()` is asynchronous.** It drops the message into a memory buffer and returns immediately. `flush()` blocks until everything is really delivered. If the script exits before flushing, the buffered messages are lost.
- **Kafka stores bytes and doesn't care what's inside.** Turning a dict into JSON and then bytes is your job, and nothing in Kafka will stop a broken message.

`acks` controls how sure you are that a message was stored: `0` = don't wait, `1` = the leader wrote it, `all` = every in-sync replica wrote it.

> **Remember:** the contract is the only thing protecting Porthos from your bugs.

### 2.2 Airflow

| Airflow word        | Meaning                                                                        |
| ------------------- | ------------------------------------------------------------------------------ |
| **DAG**       | the recipe: which steps, in which order, on which schedule                     |
| **Task**      | one step of the recipe                                                         |
| **Scheduler** | the cook who decides*when* each DAG runs                                     |
| **DAG run**   | one execution, tied to a*data interval* ("the 3rd of October"), not to "now" |
| **XCom**      | small values passed between tasks (counts, ids). Never whole datasets          |

Three rules that come from how Airflow works:

1. **Airflow re-reads your DAG file every few seconds** to discover changes. Anything at the top level of the file (API calls, heavy imports, DB connections) runs constantly. Keep all real work *inside* tasks.
2. **Tasks get retried**, so they must be **idempotent**: running the same task twice must not break anything.
3. **Airflow orchestrates; it doesn't do the work.** Your logic lives in your `ingestion` package, and the DAG only calls it.

> **Remember:** if your code only works inside Airflow, it's wrongly designed. It must run with `python -m ingestion.run` first.

A toy DAG (not yours) to learn the TaskFlow syntax:

```python
from datetime import datetime
from airflow.sdk import dag, task          # Airflow 3. In Airflow 2: from airflow.decorators import dag, task

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False, tags=["toy"])
def toy_numbers():
    @task
    def make_numbers() -> list[int]:
        return [1, 2, 3]

    @task
    def total(numbers: list[int]) -> int:
        return sum(numbers)

    total(make_numbers())   # this line defines the dependency: make_numbers → total

toy_numbers()
```

### 2.3 Data contracts

The contract is a promise between you (the producer) and Porthos (the consumer): field names, types, and which fields are required. You'll write it as a [JSON Schema](https://json-schema.org/learn/getting-started-step-by-step). Toy example:

```json
{
  "type": "object",
  "required": ["id", "email"],
  "properties": {
    "id":    { "type": "string", "format": "uuid" },
    "email": { "type": "string" },
    "age":   { "type": ["integer", "null"] }
  },
  "additionalProperties": false
}
```

| Change                                   | Safe?                                                   |
| ---------------------------------------- | ------------------------------------------------------- |
| Add an**optional** field           | ✅ compatible                                           |
| Rename, remove, or change a field's type | ❌ breaking: Porthos's job fails or silently loses data |

> **Remember:** in data science, "I'll fix it downstream" is a habit. Here, it becomes Porthos's bug at 2 a.m.

### 2.4 Pure functions: the secret to testable code

Split *thinking* (pure functions: data in, data out) from *doing* (I/O: network, Kafka, files):

```
api_client.py   I/O (network)       → test with mocked HTTP
transform.py    PURE (dict → dict)  → test with plain asserts    ← most of your logic belongs here
producer.py     I/O (Kafka)         → mock it in unit tests, use real Kafka in the integration test
```

A toy test (not your code) showing the two patterns you'll use most:

```python
# tests/test_prices.py
import pytest
from shop.prices import parse_price      # parse_price("12,50 €") -> 12.5

@pytest.mark.parametrize("text, expected", [
    ("12,50 €", 12.5),
    ("3 €", 3.0),
    ("  7,00€ ", 7.0),
])
def test_parse_price(text, expected):
    assert parse_price(text) == expected

def test_parse_price_rejects_garbage():
    with pytest.raises(ValueError):
        parse_price("free")
```

And mocking HTTP with the `responses` library (again a toy):

```python
import pytest, requests, responses

def get_temperature(city: str) -> float:
    r = requests.get(f"https://api.example.com/weather/{city}", timeout=5)
    r.raise_for_status()
    return r.json()["temp"]

@responses.activate
def test_get_temperature():
    responses.get("https://api.example.com/weather/paris", json={"temp": 21.5})
    assert get_temperature("paris") == 21.5

@responses.activate
def test_server_error_raises():
    responses.get("https://api.example.com/weather/paris", status=500)
    with pytest.raises(requests.HTTPError):
        get_temperature("paris")
```

### 2.5 Fail loudly

In the original project, an exception was caught, logged and ignored inside a loop. The task ran for 60 seconds, sent **zero** messages, and Airflow showed it as **green**. Your rules:

- Catch only **specific** exceptions you know how to handle (e.g. a timeout → retry).
- **Count** failures, and if too many fail, raise.
- A task that did nothing useful must turn **red**.

> **Remember:** a red task at 3 a.m. is good news compared to a green task that lied.

### 2.6 Notebook habits → production habits

| In a notebook you…                       | In production you…                                         |
| ----------------------------------------- | ----------------------------------------------------------- |
| run cells in any order, with hidden state | write functions that run top to bottom, with no globals     |
| `print()` to inspect                    | use`logging`, plus tests that check results automatically |
| re-run when it fails                      | add retries, and fail loudly when retries run out           |
| hard-code paths, hosts and sizes          | read them from environment variables (`config.py`)        |
| trust "it worked on my sample"            | write tests with edge-case fixtures                         |
| keep one big notebook                     | keep small modules that each do one job                     |
| wrap things in`try: … except: pass`    | catch specific errors, re-raise or count them               |

---

## 3. Get your machine ready

I checked your PC on 2026-10-03:

| Item                   | Your status                                         | To do                                                                       |
| ---------------------- | --------------------------------------------------- | --------------------------------------------------------------------------- |
| Git (Windows)          | ✅ 2.47                                             |                                                                             |
| WSL2 + Ubuntu          | ✅ installed, with Python 3.12.3, venv and git 2.43 | set`core.autocrlf` (below)                                                |
| Docker Desktop         | ⚠️ installed (28.3) but**not running**      | start it, then*Settings → Resources → WSL integration → enable Ubuntu* |
| WSL memory             | ⚠️ no`.wslconfig` (your PC has 15.4 GB)         | create one (below)                                                          |
| VS Code                | ✅                                                  | install the**WSL**, **Python** and **Jupyter** extensions |
| Python 3.14 on Windows | too new for Airflow                                 | **don't use it for this project.** Use WSL's Python 3.12              |
| Java                   | not installed                                       | not needed for your slice                                                   |

**1. Limit WSL's memory.** Create `C:\Users\zikob\.wslconfig` containing:

```ini
[wsl2]
memory=8GB
```

Then restart WSL from PowerShell with `wsl --shutdown`.

**2. Inside WSL (open the Ubuntu terminal):**

```bash
git config --global core.autocrlf input
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
mkdir -p ~/code && cd ~/code
git clone <your-team-repo-url> three-musketeers
code three-musketeers          # opens VS Code connected to WSL
```

**3. Temporary Kafka, so you aren't blocked while Aramis builds the real stack:**

```bash
docker run -d --name kafka-dev -p 9092:9092 apache/kafka:4.0.0
docker exec kafka-dev /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic users_created --partitions 3
docker exec -it kafka-dev /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic users_created --from-beginning
```

The last command waits and prints every message that arrives. Keep it open in a second terminal while you test your producer.

---

## 4. Your road map, step by step

### Week 1: with the team, plus your own prep

- [ ] **Before session 2:** open https://randomuser.me/api/?results=5&seed=musketeers in your browser and read the JSON carefully. Skim the API's [documentation](https://randomuser.me/documentation) (look at `results`, `seed`, `nat`, `inc`/`exc`). Then draft the contract table from README §3.2 with your own corrections.
- [ ] **In session 2:** lead the contract meeting, since you're its owner. Bring these questions:
  - The API returns `login.password`, `salt`, `md5`, `sha1`, `sha256`. Should any of that go downstream? (Data minimization: only send what someone needs.)
  - Should `id` be a *new random UUID on every fetch*, or come from the source (`login.uuid`)? **Think about it:** what happens in Cassandra if the same user is fetched twice? Which choice makes re-runs idempotent?
- [ ] **After session 2:** open the contract PR (draft v1). It's fine if week 2 changes it.

### Week 2: from notebook to package

**Step 1: profile the source** (`ingestion/notebooks/01_profile_randomuser.ipynb`). This is your home turf, so enjoy it. Fetch ~500 users with a fixed seed, flatten them with `pd.json_normalize`, and answer:

- [ ] Which fields have **more than one Python type** across records? (try `df[col].map(type).value_counts()`)
- [ ] How does `location.postcode`'s type depend on `nat` (nationality)?
- [ ] What's the format and timezone of `dob.date` and `registered.date`? Any surprising ranges?
- [ ] Which fields can be null or empty? (Look at the API's own `id.value`.)
- [ ] Are `email`, `login.username` and `login.uuid` unique across your 500 rows?
- [ ] Which names and phone formats would break a naive regex? (accents, non-Latin scripts)

Write your findings as markdown cells at the top. They justify every decision in the contract.

> ⚠️ Before committing the notebook, **clear its outputs** (or install `nbstripout`). Don't commit 500 rows of personal-looking data.

**Step 2: refine the contract** with what you found, and update the PR.

**Step 3: the package skeleton:**

```
ingestion/
├── pyproject.toml
├── src/ingestion/
│   ├── __init__.py
│   └── transform.py
└── tests/
    ├── fixtures/          ← real API responses saved as .json
    └── test_transform.py
```

A minimal `pyproject.toml` to start from:

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "ingestion"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["requests", "kafka-python"]

[project.optional-dependencies]
dev = ["pytest", "jsonschema", "responses"]
```

Then, inside `ingestion/`, create a venv and install the package in editable mode:

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]"
```

**Step 4: `transform.py`, test-first.**

- [ ] Save 3–5 real API responses into `tests/fixtures/`. Pick them so they cover a numeric postcode, a non-Latin name, and anything strange your profiling found.
- [ ] Write **one** test first, for one fixture and one field. Run it and watch it fail. Write just enough code to pass it. Then add the next case. This is a light form of test-driven development (TDD).
- [ ] Add a test that validates *every* fixture's output against the contract with `jsonschema.validate`.

<details><summary>Hint: if you don't know where to start with <code>to_contract</code></summary>

Start with the signature `def to_contract(raw: dict) -> dict:` and return a dict containing only `first_name`. Make one test pass. Then add one field at a time, each with its own test. The trickiest parts (postcode type, building the address string, dates) deserve their own small helper functions, each with a `parametrize`d test.

</details>

**Step 5: unblock Porthos.** As soon as `to_contract` works, generate and commit:

- [ ] `contracts/samples/sample_messages.jsonl`: ~50 valid messages, one JSON object per line
- [ ] `contracts/samples/bad_messages.jsonl`: ~5 deliberately broken ones (missing field, wrong type, invalid JSON), for testing dead-letter handling

Porthos needs these in week 2 to build Spark without waiting for your producer.

✅ **Checkpoint:** `pytest` is green locally and in CI, the contract PR is merged, and the samples are committed.

### Week 3: talk to the outside world

Build in this order, and don't start the next piece until the current one works on its own:

1. [ ] **`config.py`:** read `KAFKA_BOOTSTRAP_SERVERS`, `USERS_TOPIC` and `BATCH_SIZE` from env vars with defaults. Everything else imports settings from here.
2. [ ] **`api_client.py`:** session, timeout, retries with backoff, batching (`results=N`). Test it with `responses`: success, HTTP 500, timeout, invalid JSON.

    <details><summary>Hint: retries without writing a loop</summary>

    `requests` can retry for you: mount an `HTTPAdapter(max_retries=Retry(...))` from `urllib3.util` on your session. Read about `total`, `backoff_factor` and `status_forcelist`. Which status codes deserve a retry, and which don't? (Is a 400 worth retrying?)

    </details>
3. [ ] **`producer.py`:** a class you use as `with UserProducer(settings) as producer: producer.send_many(records)`. It sets `key=id` and `acks="all"`, counts delivered vs. failed messages, and calls `flush()` + `close()` on exit, even if an error happened.

    <details><summary>Hint: knowing whether a message really arrived</summary>

    `send()` returns a *future*. You can either wait on it (`future.get(timeout=...)`, which is simple but slow) or attach callbacks (`add_callback` / `add_errback`) that increment your counters. Then `flush()` once at the end. Try both, time them, and write the result in your learning log.

    </details>
4. [ ] **`run.py`:** a command-line entry point, `python -m ingestion.run --count 50`, that runs fetch → transform → validate → produce, then prints a summary (fetched / produced / failed). Watch the messages arrive in your `kafka-console-consumer` window.
5. [ ] **The DAG,** only now. `ingestion/dags/user_ingestion.py` is a thin TaskFlow DAG that calls your package. Put retries in `default_args`, use `catchup=False`, and add a `doc_md` explaining what it does. Ask Aramis how the package gets into the Airflow image.

✅ **Checkpoint:** stop Kafka (`docker stop kafka-dev`) and trigger the DAG. **It must turn red.** Start Kafka again and re-trigger: you get exactly N messages.

### Weeks 4–5

Follow the README ([§5](../../README.md#athos)): end-to-end run with Porthos, logging without PII, the dead-letter topic, and your data-quality DAG.

For the quality DAG, start by writing the rules in plain English in `docs/data-quality-rules.md`, *before* writing code. Example: *"Null rate of `email` must be 0%."* For each rule, decide: is it a hard failure (stop the pipeline) or a warning? That's a data-science judgment call, and it's yours to make.

---

## 5. Traps waiting for you

| Symptom                                                         | Likely cause                                                    | Fix                                                                               |
| --------------------------------------------------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `TypeError: Object of type UUID is not JSON serializable`     | a`uuid.UUID` object (same for `datetime`) inside the dict   | convert to`str` before `json.dumps`                                           |
| Script ends, but no messages appear in Kafka                    | no`flush()` before exit                                       | flush in`__exit__` / `finally`                                                |
| Works from your laptop, but Airflow gets`NoBrokersAvailable`  | inside a container,`localhost` means *the container itself* | use`kafka:29092` from containers, via the env var                               |
| DAG doesn't appear in the Airflow UI                            | the DAG file fails to import                                    | read the import-error banner in the UI, or run`airflow dags list-import-errors` |
| API gets called constantly even with the DAG paused             | code at the top level of the DAG file                           | move it inside a task                                                             |
| `ModuleNotFoundError: No module named 'ingestion'` in Airflow | the package isn't installed in the Airflow image                | coordinate with Aramis on the Dockerfile                                          |
| Tests pass, but production records fail validation              | your fixtures don't cover all nationalities                     | profile with more data, add fixtures                                              |
| randomuser.me is slow or errors out                             | it's a free public service                                      | use fixtures in tests, request in batches, retry with backoff, never hammer it    |
| `.venv/` or notebook outputs show up in `git status`        | missing ignore rules / outputs not stripped                     | `.gitignore` + `nbstripout`                                                   |

---

## 6. Working with Porthos and Aramis

**What others need from you:**

| Deliverable                                                       | For                      | When          |
| ----------------------------------------------------------------- | ------------------------ | ------------- |
| Contract draft v1                                                 | both                     | end of week 1 |
| `sample_messages.jsonl` + `bad_messages.jsonl`                | Porthos                  | early week 2  |
| An installable`ingestion` package (`pip install ./ingestion`) | Aramis (Airflow image)   | end of week 3 |
| A reusable producer (`UserProducer`)                            | Aramis (end-to-end test) | week 4        |
| Data-quality rules                                                | Porthos (code review)    | week 5        |

**What you need from them:**

| From    | What                                                                        | When                                     |
| ------- | --------------------------------------------------------------------------- | ---------------------------------------- |
| Aramis  | the real Kafka stack, CI, the Airflow image                                 | weeks 2–3 (use`kafka-dev` until then) |
| Porthos | code reviews on every PR; the Cassandra table design (for your quality DAG) | from week 2                              |

**Suggested pairing sessions** (you type, they talk):

- Week 2 with **Porthos:** package skeleton + your first tests.
- Week 3 with **Aramis:** the producer and the DAG.

**Asking for help well:** say *what you tried*, give *the exact error* (copy-paste it, don't paraphrase), and say *what you expected instead*. Most problems get solved while you write that message.

---

## 7. Learning order

| When      | Topic                            | Resource                                                                                                                                                                                   |
| --------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Week 1    | Git, WSL, Docker basics          | learngitbranching.js.org, Docker*Get started*                                                                                                                                            |
| Week 1    | The big picture                  | *Fundamentals of Data Engineering*, ch. 1–2                                                                                                                                             |
| Week 2    | Packaging + pytest               | [packaging.python.org](https://packaging.python.org/) (*src layout*), [pytest docs](https://docs.pytest.org/)                                                                              |
| Week 2    | JSON Schema                      | [json-schema.org: Getting started](https://json-schema.org/learn/getting-started-step-by-step)                                                                                              |
| Week 3    | Kafka producer                   | [Kafka docs: Producer configs](https://kafka.apache.org/documentation/#producerconfigs) (`acks`, `linger.ms`, `batch.size`)                                                           |
| Week 3    | Airflow                          | [Core Concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html), [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) |
| Week 4–5 | Ingestion & orchestration theory | *Fundamentals of Data Engineering*, the ingestion and orchestration chapters                                                                                                             |
| Week 5    | Logging                          | Python docs:*Logging HOWTO*                                                                                                                                                              |
| Ongoing   | Team book                        | *Designing Data-Intensive Applications*, ch. 11 when the team reaches it                                                                                                                 |

---

## 8. Your learning-log template

Copy this into `docs/learning-log/athos.md` after each work session:

```markdown
## 2026-10-__: <what I worked on>
- **Learned:**
- **Confused by:**
- **Next, I'll look up:**
```

---

## 9. You've done your role when you can…

- [ ] explain to a non-technical friend what your part of the pipeline does, and why it can fail;
- [ ] answer the 8 self-check questions in your README section without notes;
- [ ] stop Kafka in the middle of a DAG run and **predict exactly** what your code will do before you look;
- [ ] show a teammate a PR you're proud of, with tests, and explain every line.
