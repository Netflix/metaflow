import os
import json
from datetime import datetime

###
# Algorithm to determine 1st error:
# ignore the failures where message = ""
# group the failures via templateName
# sort each group by finishedAt
# find the group for which the last finishedAt is earliest
# if the earliest message is "No more retries left" then get the n-1th message from that group
# else return the last message.
###


def sort_tasks_by_finished_at(data: dict):
    for key, value in data.items():
        data[key] = sorted(
            value,
            key=lambda x: datetime.strptime(x["finishedAt"], "%Y-%m-%dT%H:%M:%SZ"),
        )


def find_key_with_smallest_largest_finished_at(data: dict) -> str:
    smallest_largest_date = None
    smallest_largest_key = None

    for key, value in data.items():
        largest_finished_at = datetime.strptime(
            value[-1]["finishedAt"], "%Y-%m-%dT%H:%M:%SZ"
        )
        if smallest_largest_date is None or largest_finished_at < smallest_largest_date:
            smallest_largest_date = largest_finished_at
            smallest_largest_key = key

    return smallest_largest_key


def determine_first_error() -> str:
    workflow_failures = os.getenv("METAFLOW_ARGO_WORKFLOW_FAILURES", "[]")
    workflow_failures_json = json.loads(
        json.loads(workflow_failures, strict=False), strict=False
    )

    valid_workflow_failures = []

    for wf in workflow_failures_json:
        # ignore any workflow failures whose message is empty
        if "message" in wf and wf["message"] != "":
            valid_workflow_failures.append(wf)

    # it should never be the case that the workflow has failed but
    # message field in every object is empty
    if len(valid_workflow_failures) == 0:
        return

    template_dict = {}
    # group the failures by the templateName property
    for wf in valid_workflow_failures:
        if wf["templateName"] not in template_dict:
            template_dict[wf["templateName"]] = []
        templates = template_dict[wf["templateName"]]
        templates.append(wf)
        template_dict[wf["templateName"]] = templates

    # sort each group by finishedAt
    sort_tasks_by_finished_at(template_dict)
    last_finished_at_dict = {}
    # find the group for which the last finishedAt is earliest..
    for wf_template_name, wf in template_dict.items():
        if wf_template_name not in last_finished_at_dict:
            last_finished_at_dict[wf_template_name] = []
        templates = last_finished_at_dict[wf_template_name]
        templates.append(wf[-1])
        last_finished_at_dict[wf_template_name] = templates

    sort_tasks_by_finished_at(last_finished_at_dict)
    candidate_template_name = find_key_with_smallest_largest_finished_at(
        last_finished_at_dict
    )

    if template_dict[candidate_template_name][-1]["message"] == "No more retries left":
        return template_dict[candidate_template_name][-2]["message"]
    else:
        return template_dict[candidate_template_name][-1]["message"]


if __name__ == "__main__":
    determine_first_error()
