# Upstream PR — Defer cron timetable description (remove cron_descriptor/gettext from the scheduling hot path)

Prepared for a pull request against **apache/airflow** (`main`). One of two independent,
complementary DAG-parsing performance PRs (the other: *cache `decode_timetable`*).

---

## Title

```
Compute CronMixin.description lazily to keep cron_descriptor off the scheduling hot path
```

## Affected

- Any deployment with many cron-scheduled DAGs. Cost scales with DAG count and shows up in
  the **dag-processor** and **scheduler** (both compute `next_dagrun_info` per DAG).
- Confirmed on 3.2.2; the same code exists on `main`.

## Problem

Every time a cron timetable is constructed (`CronDataIntervalTimetable`,
`CronTriggerTimetable`), `CronMixin.__init__` eagerly builds a **human-readable English
description** of the cron expression via `cron_descriptor` — which loads **gettext**
translation catalogs and compiles. This happens for every DAG, every parse cycle, during
`next_dagrun_info` computation — even though scheduling never reads the description (it's a
display-only field used by the UI/API).

On a dag-processor pass over 700 cron DAGs this is a large, pure-CPU cost in the per-DAG loop
that blocks the job heartbeat, and it is one of the contributors to liveness-probe failures
on busy instances.

## Root cause

`airflow/timetables/_cron.py`:

```python
class CronMixin:
    def __init__(self, cron, timezone):
        self._expression = cron_presets.get(cron, cron)
        ...
        try:
            if len(croniter(self._expression).expanded) > 5:
                raise FormatException()
            self.description = self._describe_with_dom_dow_fix(self._expression)   # cron_descriptor + gettext
        except (CroniterBadCronError, FormatException, MissingFieldException):
            self.description = ""
```

`_describe_with_dom_dow_fix` → `cron_descriptor.ExpressionDescriptor(...).get_description()`
→ gettext catalog parse + `compile`. The scheduling methods (`_get_next`, `_get_prev`,
`next_dagrun_info`) use only `self._expression`, `self._timezone` and `croniter` — never
`self.description`.

## Fix

Make `description` a `functools.cached_property` so it is computed (and memoized) only on
first access — which, for the scheduler/dag-processor, is never.

### Diff (against 3.2.2 `airflow/timetables/_cron.py`)

```diff
 from __future__ import annotations

 import datetime
+import functools
 from typing import TYPE_CHECKING
```

```diff
     def __init__(self, cron: str, timezone: str | Timezone | FixedTimezone) -> None:
         self._expression = cron_presets.get(cron, cron)

         if isinstance(timezone, str):
             timezone = parse_timezone(timezone)
         self._timezone = timezone

-        try:
-            # checking for more than 5 parameters in Cron and avoiding evaluation for now,
-            # as Croniter has inconsistent evaluation with other libraries
-            if len(croniter(self._expression).expanded) > 5:
-                raise FormatException()
-
-            self.description = self._describe_with_dom_dow_fix(self._expression)
-
-        except (CroniterBadCronError, FormatException, MissingFieldException):
-            self.description = ""
+    @functools.cached_property
+    def description(self) -> str:
+        # Building the human-readable cron description (cron_descriptor + gettext) is only
+        # needed for display, so defer it off the scheduling hot path.
+        try:
+            # checking for more than 5 parameters in Cron and avoiding evaluation for now,
+            # as Croniter has inconsistent evaluation with other libraries
+            if len(croniter(self._expression).expanded) > 5:
+                raise FormatException()
+            return self._describe_with_dom_dow_fix(self._expression)
+        except (CroniterBadCronError, FormatException, MissingFieldException):
+            return ""
```

## Why it's safe

- `description` is a **derived display field** — not part of the timetable's `serialize()`
  (which emits only `expression` + `timezone`), so serialization output is unchanged.
- Same value, same error handling (`""` on invalid cron) — just computed on demand and cached.
- No API change: `timetable.description` still returns the identical string.
- `cached_property` memoizes per instance, so even display callers pay at most once.

## Measured

Reproduced with 700 cron DAGs (one `airflow dags reserialize` pass = one parse+serialize+write
cycle), Airflow 3.2.2, forcing fresh bytecode so the patch is guaranteed active:

| Case | CPU (user time) | vs baseline |
|---|---|---|
| baseline | 29.1s | — |
| **this fix** | 20.9s | **~28% less** |

A sanity-confirmed clean run measured 25.2s → 19.3s CPU with this + the decode cache applied.
cProfile confirms `gettext._parse` (~11,200 calls) and the associated `compile` leave the hot
path entirely after this change. On a throttled 1-CPU dag-processor the absolute saving is
larger, directly shrinking the heartbeat-blocking window.

## Testing

- Unit: assert `CronDataIntervalTimetable("0 0 * * *", "UTC").description` returns the expected
  text (unchanged), and that an invalid cron yields `""`.
- Assert `cron_descriptor.ExpressionDescriptor` is **not** invoked during
  `next_dagrun_info` (e.g. patch/spy it and confirm zero calls when only scheduling).

## Related (separate PR)

Caching `decode_timetable` by serialized content further reduces repeated timetable
construction; independent of this change (different file). This PR stands alone.

## Commit message

```
Compute CronMixin.description lazily to keep cron_descriptor off the hot path

CronMixin.__init__ eagerly built the human-readable cron description via
cron_descriptor (which loads gettext catalogs and compiles) on every
construction. The scheduler and dag-processor construct cron timetables for
every DAG while computing next_dagrun_info but never read .description, so this
was pure per-DAG CPU on the hot path that blocks the job heartbeat.

Make description a cached_property so it is computed (and memoized) only when
actually accessed (UI/API). description is a derived display field and is not
part of the timetable's serialize() output, so behaviour is unchanged.

Reserializing 700 cron DAGs drops ~28% CPU in local measurement.
```

## PR checklist (apache/airflow)

- [ ] Open a GitHub issue describing the per-DAG cron_descriptor overhead and link it.
- [ ] Unit test: description unchanged + not computed during scheduling.
- [ ] Newsfragment (performance/bugfix) if required by the version.
- [ ] Target `main`; maintainers backport as applicable.
