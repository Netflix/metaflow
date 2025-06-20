import json
from metaflow.metadata_provider import MetaDatum


def _save_metadata(
    metadata_provider,
    run_id,
    step_name,
    task_id,
    attempt_id,
    card_uuid,
    save_metadata,
):
    entries = [
        MetaDatum(
            field=card_uuid,
            value=json.dumps(save_metadata),
            type="card-info",
            tags=["attempt_id:{0}".format(attempt_id)],
        )
    ]
    metadata_provider.register_metadata(run_id, step_name, task_id, entries)
