# Examples

This folder contains two runnable examples for `aiocluster`.

## Prerequisites

- Python 3.13+
- `uv` installed

From the repository root (`aiocluster`), run examples with:

```bash
uv run python <example>
```

## 1 `simple.py`

Starts three in-process cluster nodes (`simple1`, `simple2`, `simple3`) that gossip with each other over localhost ports `7000-7002`.


Run:

```bash
uv run python examples/simple.py
```

## 2 `api/app.py`

Starts three FastAPI applications (`8000`, `8001`, `8002`), each backed by an `aiocluster` node (`7000`, `7001`, `7002`)
that gossip between each other and share key/values.

Run:

```bash
uv run python examples/api/app.py
```

Open FastAPI docs:
- http://127.0.0.1:8000/docs
- http://127.0.0.1:8001/docs
- http://127.0.0.1:8002/docs

Useful endpoints:
- `GET /state` - cluster snapshot
- `PUT /kv_set` - set a key/value
- `DELETE /kv_mark` - mark a key as deleted
