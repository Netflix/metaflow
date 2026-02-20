from metaflow import namespace, Flow
from datetime import datetime
from time import sleep


def wait_for_result(triggered_run, timeout=60):
    "Wait for a TriggeredRun to have an executing run attached to it"
    slept = 0
    while triggered_run.run is None and slept < timeout:
        slept += 10
        sleep(10)

    if triggered_run.run is None:
        raise TimeoutError(
            "Waiting for flow failed. Waited for %s seconds with no results" % timeout
        )

    run = wait_for_run_to_finish(triggered_run.run, timeout)

    return run


def wait_for_run(flow_name, ns=None, timeout=60):
    "Wait for a Run for a flow name to start executing in the given namespace"
    namespace(ns)
    slept = 0
    current_ts = datetime.now()
    run = None
    while slept < timeout:
        try:
            latest_run = Flow(flow_name).latest_run
        except Exception:
            latest_run = None

        if latest_run is not None and latest_run.created_at > current_ts:
            run = latest_run
            break
        slept += 10
        sleep(10)

    if run is None:
        raise TimeoutError(
            "Found no new run in the span of %s seconds. Timed out." % timeout
        )

    namespace(None)
    return run


def wait_for_runs_after_ts(
    flow_name, ns=None, after_ts=None, expected_runs=1, timeout=60
):
    """
    Wait for a number of runs for a flow name to start executing
    after a given timestamp in the specified namespace
    """
    namespace(ns)
    current_ts = after_ts or datetime.now()
    runs = []
    pathspecs = set()
    slept = 0
    while len(runs) < expected_runs and slept < timeout:
        try:
            flow = Flow(flow_name)
        except Exception:
            flow = None

        if flow is not None:
            for run in flow.runs():
                if run.created_at < current_ts:
                    break  # we're iterating too old runs already

                if run.pathspec in pathspecs:
                    continue  # already covered this run

                pathspecs.add(run.pathspec)
                runs.append(run)
                if len(runs) == expected_runs:
                    break  # we have enough runs gathered
        if len(runs) == expected_runs:
            break  # we have enough runs gathered

        slept += 10
        sleep(10)

    if len(runs) != expected_runs:
        raise TimeoutError(
            "Could not gather %s runs in %s seconds" % (expected_runs, timeout)
        )

    namespace(None)
    return runs


def wait_for_run_to_finish(run, timeout=120):
    "Wait for a Run to finish"
    slept = 0
    while not run.finished_at and slept < timeout:
        slept += 10
        sleep(10)

    if not run.finished_at:
        raise TimeoutError(
            "Triggered run did not finish in time. Waited for %s seconds for the run to finish"
            % timeout
        )

    # We record an exception but finish test flows in order to get a faster test result,
    # instead of having to wait for a timeout
    test_failure = getattr(run.data, "test_failure", None)
    if test_failure is not None:
        raise test_failure

    return run
