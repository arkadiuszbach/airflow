# Upstream PR — Fix triggerer crash when DB connection drops during `Trigger.clean_unused`

Prepared for a pull request against **apache/airflow** (`main`).

---

## Title

```
Fix triggerer crash when DB connection drops during Trigger.clean_unused
```

## Affected

- All Airflow 3.x with the asyncio triggerer (`TriggererJobRunner`).
- Confirmed on 3.2.2; the same code path exists on `main`.

## Problem

When the metadata DB connection drops briefly (e.g. failover, pgbouncer restart,
`server closed the connection unexpectedly`) while the triggerer is running
`Trigger.clean_unused()`, the triggerer process crashes and the pod restarts.

Full traceback (triggerer, Airflow 3.2.2):

```
2026-09-14T06:21:13.416868Z [error    ] Exception when executing TriggerRunnerSupervisor.run [airflow.jobs.triggerer_job_runner.TriggererJobRunner] loc=triggerer_job_runner.py:221
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/jobs/triggerer_job_runner.py", line 218, in _execute
    self.trigger_runner.run()
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/jobs/triggerer_job_runner.py", line 614, in run
    self.clean_unused()
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/jobs/triggerer_job_runner.py", line 661, in clean_unused
    Trigger.clean_unused()
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/utils/session.py", line 100, in wrapper
    return func(*args, session=session, **kwargs)  # type: ignore[arg-type]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/trigger.py", line 230, in clean_unused
    for attempt in run_with_db_retries():
                   ^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/tenacity/__init__.py", line 438, in __iter__
    do = self.iter(retry_state=retry_state)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/tenacity/__init__.py", line 371, in iter
    result = action(retry_state)
             ^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/tenacity/__init__.py", line 393, in <lambda>
    self._add_action_func(lambda rs: rs.outcome.result())
                                     ^^^^^^^^^^^^^^^^^^^
  File "/usr/python/lib/python3.12/concurrent/futures/_base.py", line 449, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "/usr/python/lib/python3.12/concurrent/futures/_base.py", line 401, in __get_result
    raise self._exception
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/trigger.py", line 232, in clean_unused
    session.execute(
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/session.py", line 2351, in execute
    return self._execute_internal(
           ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/session.py", line 2249, in _execute_internal
    result: Result[Any] = compile_state_cls.orm_execute_statement(
                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/bulk_persistence.py", line 1660, in orm_execute_statement
    return super().orm_execute_statement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/context.py", line 306, in orm_execute_statement
    result = conn.execute(
             ^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1419, in execute
    return meth(
           ^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 527, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1641, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1813, in _execute_context
    conn = self._revalidate_connection()
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 673, in _revalidate_connection
    self._invalid_transaction()
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 663, in _invalid_transaction
    raise exc.PendingRollbackError(
sqlalchemy.exc.PendingRollbackError: Can't reconnect until invalid transaction is rolled back.  Please rollback() fully before proceeding (Background on this error at: https://sqlalche.me/e/20/8s2b)
2026-09-14T06:21:13.421641Z [info     ] Waiting for triggers to clean up [airflow.jobs.triggerer_job_runner.TriggererJobRunner] loc=triggerer_job_runner.py:224
2026-09-14T06:21:13.527625Z [info     ] Process exited                 [supervisor] exit_code=0 loc=supervisor.py:860 pid=29 signal_sent=SIGINT
2026-09-14T06:21:13.527858Z [info     ] Exited trigger loop            [airflow.jobs.triggerer_job_runner.TriggererJobRunner] loc=triggerer_job_runner.py:229
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/jobs/job.py", line 355, in run_job
    return execute_job(job, execute_callable=execute_callable)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/jobs/job.py", line 384, in execute_job
    ret = execute_callable()
          ^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/jobs/triggerer_job_runner.py", line 218, in _execute
    self.trigger_runner.run()
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/jobs/triggerer_job_runner.py", line 614, in run
    self.clean_unused()
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/jobs/triggerer_job_runner.py", line 661, in clean_unused
    Trigger.clean_unused()
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/utils/session.py", line 100, in wrapper
    return func(*args, session=session, **kwargs)  # type: ignore[arg-type]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/trigger.py", line 230, in clean_unused
    for attempt in run_with_db_retries():
                   ^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/tenacity/__init__.py", line 438, in __iter__
    do = self.iter(retry_state=retry_state)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/tenacity/__init__.py", line 371, in iter
    result = action(retry_state)
             ^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/tenacity/__init__.py", line 393, in <lambda>
    self._add_action_func(lambda rs: rs.outcome.result())
                                     ^^^^^^^^^^^^^^^^^^^
  File "/usr/python/lib/python3.12/concurrent/futures/_base.py", line 449, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "/usr/python/lib/python3.12/concurrent/futures/_base.py", line 401, in __get_result
    raise self._exception
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/trigger.py", line 232, in clean_unused
    session.execute(
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/session.py", line 2351, in execute
    return self._execute_internal(
           ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/session.py", line 2249, in _execute_internal
    result: Result[Any] = compile_state_cls.orm_execute_statement(
                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/bulk_persistence.py", line 1660, in orm_execute_statement
    return super().orm_execute_statement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/context.py", line 306, in orm_execute_statement
    result = conn.execute(
             ^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1419, in execute
    return meth(
           ^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 527, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1641, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1813, in _execute_context
    conn = self._revalidate_connection()
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 673, in _revalidate_connection
    self._invalid_transaction()
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 663, in _invalid_transaction
    raise exc.PendingRollbackError(
sqlalchemy.exc.PendingRollbackError: Can't reconnect until invalid transaction is rolled back.  Please rollback() fully before proceeding (Background on this error at: https://sqlalche.me/e/20/8s2b)

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1967, in _exec_single_context
    self.dialect.do_execute(
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 952, in do_execute
    cursor.execute(statement, parameters)
psycopg2.OperationalError: server closed the connection unexpectedly
	This probably means the server terminated abnormally
	before or while processing the request.


The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/airflow/.local/bin/airflow", line 6, in <module>
    sys.exit(main())
             ^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/__main__.py", line 55, in main
    args.func(args)
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/cli/cli_config.py", line 49, in command
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/utils/cli.py", line 113, in wrapper
    return f(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/utils/providers_configuration_loader.py", line 54, in wrapped_function
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/cli/commands/triggerer_command.py", line 90, in triggerer
    run_command_with_daemon_option(
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/cli/commands/daemon_utils.py", line 86, in run_command_with_daemon_option
    callback()
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/cli/commands/triggerer_command.py", line 93, in <lambda>
    callback=lambda: triggerer_run(args.skip_serve_logs, args.capacity, triggerer_heartrate, queues),
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/utils/memray_utils.py", line 60, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/cli/commands/triggerer_command.py", line 61, in triggerer_run
    run_job(job=triggerer_job_runner.job, execute_callable=triggerer_job_runner._execute)
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/utils/session.py", line 100, in wrapper
    return func(*args, session=session, **kwargs)  # type: ignore[arg-type]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/jobs/job.py", line 357, in run_job
    job.complete_execution(session=session)
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/utils/session.py", line 98, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/jobs/job.py", line 285, in complete_execution
    session.merge(self)
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/session.py", line 3945, in merge
    return self._merge(
           ^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/session.py", line 4023, in _merge
    merged = self.get(
             ^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/session.py", line 3680, in get
    return self._get_impl(
           ^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/session.py", line 3859, in _get_impl
    return db_load_fn(
           ^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/loading.py", line 695, in load_on_pk_identity
    session.execute(
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/session.py", line 2351, in execute
    return self._execute_internal(
           ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/session.py", line 2249, in _execute_internal
    result: Result[Any] = compile_state_cls.orm_execute_statement(
                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/orm/context.py", line 306, in orm_execute_statement
    result = conn.execute(
             ^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1419, in execute
    return meth(
           ^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 527, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1641, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1846, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1986, in _exec_single_context
    self._handle_dbapi_exception(
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2363, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1967, in _exec_single_context
    self.dialect.do_execute(
  File "/home/airflow/.local/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 952, in do_execute
    cursor.execute(statement, parameters)
sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) server closed the connection unexpectedly
	This probably means the server terminated abnormally
	before or while processing the request.

[SQL: SELECT job.id AS job_id, job.dag_id AS job_dag_id, job.state AS job_state, job.job_type AS job_job_type, job.start_date AS job_start_date, job.end_date AS job_end_date, job.latest_heartbeat AS job_latest_heartbeat, job.executor_class AS job_executor_class, job.hostname AS job_hostname, job.unixname AS job_unixname 
FROM job 
WHERE job.id = %(pk_1)s]
[parameters: {'pk_1': 1731}]
(Background on this error at: https://sqlalche.me/e/20/e3q8)
```

## Root cause

`Trigger.clean_unused()` retries its first statement using the bare
`run_with_db_retries()` tenacity iterator:

```python
for attempt in run_with_db_retries():
    with attempt:
        session.execute(update(TaskInstance)...)
```

`run_with_db_retries()` only returns a configured `tenacity.Retrying` object — it has
**no reference to the session and never calls `session.rollback()`** between attempts.

1. Attempt 1 fails with `OperationalError` (connection dropped mid-statement). The
   session's transaction is now *invalidated*.
2. Attempt 2 runs without a rollback → SQLAlchemy raises `PendingRollbackError`.
3. `PendingRollbackError` is a subclass of `InvalidRequestError`, **not** `DBAPIError`,
   so tenacity's `retry_if_exception_type((DBAPIError, StaleDataError))` does not match
   it and (with `reraise=True`) it propagates out of the trigger loop, crashing the
   triggerer.

`@provide_session` does not help: its `create_session()` rolls back only at the
function boundary — i.e. *after* the retry loop has already given up — so it cleans the
session on the way out but cannot let the retry succeed.

The project already has the correct primitive for this: `retry_db_transaction`, which
wraps the same `run_with_db_retries()` loop but rolls back the session on
`DBAPIError`/`StaleDataError` before re-raising, so the next attempt starts on a clean
transaction.

## Fix

Decorate `Trigger.clean_unused` with `@retry_db_transaction` (ordered so
`provide_session` creates one stable session that the retry decorator rolls back
between attempts) and drop the bare retry loop. Because `clean_unused` performs no
intermediate `commit()`, the `UPDATE` and `DELETE` run in a single transaction that is
committed once on return; both statements are idempotent, so retrying the whole method
is safe.

### Diff (against `main`, `airflow-core/src/airflow/models/trigger.py`)

```diff
-from airflow.utils.retries import run_with_db_retries
+from airflow.utils.retries import retry_db_transaction
```

```diff
     @classmethod
     @provide_session
+    @retry_db_transaction
     def clean_unused(cls, *, session: Session = NEW_SESSION) -> None:
         """
         Delete all triggers that have no tasks dependent on them and are not associated to an asset.

         Triggers have a one-to-many relationship to task instances, so we need to clean those up first.
         Afterward we can drop the triggers not referenced by anyone.
         """
         # Update all task instances with trigger IDs that are not DEFERRED to remove them
-        for attempt in run_with_db_retries():
-            with attempt:
-                session.execute(
-                    update(TaskInstance)
-                    .where(
-                        TaskInstance.state != TaskInstanceState.DEFERRED, TaskInstance.trigger_id.is_not(None)
-                    )
-                    .values(trigger_id=None)
-                )
+        session.execute(
+            update(TaskInstance)
+            .where(
+                TaskInstance.state != TaskInstanceState.DEFERRED, TaskInstance.trigger_id.is_not(None)
+            )
+            .values(trigger_id=None)
+        )

         # Get all triggers that have no task instances, assets, or callbacks depending on them and delete them
         ids = select(cls.id).where(
             ~cls.assets.any(),
             ~cls.callback.has(),
             ~cls.task_instance.has(),
         )
         ids = with_row_locks(ids, session, of=cls, skip_locked=True, key_share=False)
         if get_dialect_name(session) == "mysql":
             # MySQL doesn't support DELETE with JOIN, so we need to do it in two steps
             ids_list = list(session.scalars(ids).all())
             session.execute(
                 delete(Trigger).where(Trigger.id.in_(ids_list)).execution_options(synchronize_session=False)
             )
         else:
             session.execute(
                 delete(Trigger).where(Trigger.id.in_(ids)).execution_options(synchronize_session=False)
             )
```

> If `run_with_db_retries` is used elsewhere in the file, keep both imports instead of
> swapping. In current `main` it is only used in `clean_unused`.

## Why it's safe

- **Single transaction:** `clean_unused` does not `commit()` mid-method; `UPDATE` and
  `DELETE` commit together when the method returns. A rollback on retry discards all
  partial work cleanly.
- **Idempotent:** `UPDATE ... SET trigger_id=NULL WHERE trigger_id IS NOT NULL` and
  `DELETE FROM trigger WHERE id IN (<unreferenced>)` produce the same end state when
  re-run.
- **No new retryable surface:** `retry_db_transaction` retries the same exception set
  (`DBAPIError`, `StaleDataError`) as before; it simply adds the missing `rollback()`.
- **Established pattern:** mirrors existing uses of `retry_db_transaction` in the
  codebase (e.g. `dag_processing/manager.py`).

Behaviour change to call out in review: the retry now covers the whole method (the
`DELETE` as well), whereas before only the `UPDATE` was retried. This is intentional and
safe per the points above.

## Testing

- Unit: add a test that patches `session.execute` to raise `OperationalError` on the
  first call and succeed on the second; assert `clean_unused` completes without raising
  and that `session.rollback()` was invoked between attempts.
- Manual/integration: run the triggerer, drop the DB connection briefly (restart
  pgbouncer / kill the backend), and confirm the triggerer continues instead of exiting
  with `PendingRollbackError`.

## Related (out of scope for this PR)

`Job.complete_execution()` → `session.merge()` makes an unprotected DB call on the
triggerer shutdown path and produced the secondary traceback. Suggest a separate issue;
with `clean_unused` fixed, the triggerer no longer reaches that shutdown path on a
transient blip.

## Commit message

```
Fix triggerer crash when DB connection drops during Trigger.clean_unused

Trigger.clean_unused() retried its UPDATE via the bare run_with_db_retries()
loop, which has no session reference and never rolls back between attempts.
When the metadata DB connection dropped mid-statement (OperationalError:
server closed the connection unexpectedly), the session's transaction was
left invalidated and the retry raised PendingRollbackError. Because
PendingRollbackError is not a DBAPIError, tenacity reraised it and the
triggerer process crashed and restarted.

Decorate clean_unused with @retry_db_transaction, which rolls back the
session on DBAPIError/StaleDataError before retrying, so the next attempt
runs on a clean transaction. clean_unused performs no intermediate commit and
both of its statements are idempotent, so retrying the whole method within a
single transaction is safe.
```

## PR checklist reminders (apache/airflow)

- [ ] Open a GitHub issue describing the bug and link it in the PR.
- [ ] Add a unit test for the rollback-on-retry behaviour.
- [ ] Add a newsfragment under `newsfragments/` (bugfix) if required by the version.
- [ ] Target branch `main`; maintainers backport to active release branches.
