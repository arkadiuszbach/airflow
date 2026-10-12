# Upstream PR — Cache `decode_timetable` by serialized content (dedupe repeated timetable deserialization)

Prepared for a pull request against **apache/airflow** (`main`). One of two independent,
complementary DAG-parsing performance PRs (the other: *lazy cron description*).

---

## Title

```
Cache decode_timetable by serialized content to avoid rebuilding identical timetables
```

## Affected

- Any deployment with many DAGs, especially when DAGs **share schedules**. Cost scales with
  DAG count and shows up in the **dag-processor** and **scheduler**.
- Confirmed on 3.2.2; the same code exists on `main`.

## Problem

During a dag-processing pass, `decode_timetable` is called **many times per DAG** (observed
~16× per DAG for 700 DAGs → ~11,200 calls) while serialized DAGs are read back and
`next_dagrun_info` is computed. Each call re-imports the timetable class and rebuilds the
timetable object (croniter construction, etc.) from scratch — even though the serialized form
is identical across those calls and across DAGs that share the same schedule (e.g. hundreds of
DAGs on `0 18 * * 1-5`).

## Root cause

`airflow/serialization/decoders.py`:

```python
def decode_timetable(var: dict[str, Any]) -> CoreTimetable:
    if is_core_timetable_import_path(importable_string := var[Encoding.TYPE]):
        timetable_type = import_string(importable_string)
    else:
        timetable_type = find_registered_custom_timetable(importable_string)
    return timetable_type.deserialize(var[Encoding.VAR])
```

No memoization — identical `var` payloads rebuild identical timetables repeatedly.

## Fix

Timetables are **immutable value objects**, so an identical serialized form always yields an
equivalent timetable. Add a **content-addressed** cache keyed by the serialized `var` itself.

### Diff (against 3.2.2 `airflow/serialization/decoders.py`)

```diff
 from __future__ import annotations

 import datetime
+import functools
+import json
 from typing import TYPE_CHECKING, Any, TypeVar
```

```diff
-def decode_timetable(var: dict[str, Any]) -> CoreTimetable:
-    """
-    Decode a previously serialized timetable.
-
-    Most of the deserialization logic is delegated to the actual type, which
-    we import from string.
-
-    :meta private:
-    """
-    if is_core_timetable_import_path(importable_string := var[Encoding.TYPE]):
-        timetable_type: type[CoreTimetable] = import_string(importable_string)
-    else:
-        timetable_type = find_registered_custom_timetable(importable_string)
-    return timetable_type.deserialize(var[Encoding.VAR])
+def _decode_timetable(var: dict[str, Any]) -> CoreTimetable:
+    if is_core_timetable_import_path(importable_string := var[Encoding.TYPE]):
+        timetable_type: type[CoreTimetable] = import_string(importable_string)
+    else:
+        timetable_type = find_registered_custom_timetable(importable_string)
+    return timetable_type.deserialize(var[Encoding.VAR])
+
+
+@functools.lru_cache(maxsize=4096)
+def _decode_timetable_cached(key: str) -> CoreTimetable:
+    return _decode_timetable(json.loads(key))
+
+
+def decode_timetable(var: dict[str, Any]) -> CoreTimetable:
+    """
+    Decode a previously serialized timetable.
+
+    Most of the deserialization logic is delegated to the actual type, which
+    we import from string.
+
+    Timetables are immutable value objects, so identical serialized forms are
+    cached (content-addressed) to avoid re-importing and rebuilding croniter for
+    every DAG -- many DAGs share the same schedule. The cache key is the
+    serialized form itself, so a changed schedule yields a different key and can
+    never return a stale timetable. Only JSON-native forms are cached; anything
+    else falls back to the uncached path.
+
+    :meta private:
+    """
+    try:
+        key = json.dumps(var, sort_keys=True)
+    except TypeError:
+        return _decode_timetable(var)
+    return _decode_timetable_cached(key)
```

## Why it's safe (addresses the "stale cache" concern)

- **The cache key is the serialized timetable content itself.** If a DAG's schedule changes,
  its serialized `var` changes → different key → cache miss → fresh build. A stale value is
  therefore impossible — the key fully determines the value.
- Timetables are **stateless/immutable** (`_get_next(current)` takes the time as an argument),
  so returning a shared instance to multiple DAGs is safe.
- **Correctness-preserving fallback:** only JSON-native payloads are cached; if `json.dumps`
  raises (non-serializable payload), it falls back to the original uncached path. The core
  timetable forms (cron, delta) are JSON-native, so they cache; anything unusual is unaffected.
- Bounded memory: `lru_cache(maxsize=4096)` — far more than the handful of distinct schedules a
  deployment actually uses.

## Measured

Reproduced with 700 DAGs (one `airflow dags reserialize` pass), Airflow 3.2.2, forcing fresh
bytecode so the patch is guaranteed active:

| Case | CPU (user time) | vs baseline |
|---|---|---|
| baseline | 29.1s | — |
| **this fix** | 19.1s | **~34% less** |

Note: this overlaps with the *lazy cron description* PR (both reduce cron-timetable construction
cost), so applying **both** gives ~36% (18.7s) rather than the sum. `decode_timetable` call
volume drops from ~11,200 to roughly the number of distinct schedules.

## Testing

- Unit: `decode_timetable(var)` returns an equivalent timetable to the uncached path for cron
  and delta timetables; two calls with the same `var` hit the cache
  (`_decode_timetable_cached.cache_info().hits >= 1`).
- Change a field in `var` → different result (no stale hit).
- Non-JSON-serializable `var` → falls back without error.

## Related (separate PR)

The *lazy cron description* change (`timetables/_cron.py`) is independent (different file) and
complementary; either can land first.

## Commit message

```
Cache decode_timetable by serialized content to avoid rebuilding timetables

decode_timetable was called many times per DAG during a dag-processing pass
(~16x/DAG in local testing) and rebuilt identical timetables from scratch each
time, including for the many DAGs that share the same schedule.

Timetables are immutable value objects, so add a content-addressed lru_cache
keyed by the serialized form. Because the key is the serialized timetable
itself, a changed schedule produces a different key and can never return a
stale timetable; only JSON-native payloads are cached, with a correctness-
preserving fallback otherwise.

Reserializing 700 DAGs drops ~34% CPU in local measurement.
```

## PR checklist (apache/airflow)

- [ ] Open a GitHub issue describing the repeated per-DAG timetable deserialization and link it.
- [ ] Unit tests: equivalence vs uncached, cache hit, content-addressed invalidation, fallback.
- [ ] Newsfragment (performance) if required by the version.
- [ ] Target `main`; maintainers backport as applicable.
