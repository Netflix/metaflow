# Lint as: python3
"""CLI interface for KFP diagnose_me tool."""

import json as json_library
import sys
from typing import Dict, Text
import click
from kfp.cli.diagnose_me import dev_env
from kfp.cli.diagnose_me import gcp
from kfp.cli.diagnose_me import kubernetes_cluster as k8
from kfp.cli.diagnose_me import utility


@click.group()
def diagnose_me():
    """Prints diagnoses information for KFP environment."""
    pass


@diagnose_me.command()
@click.option(
    '-j',
    '--json',
    is_flag=True,
    help='Output in Json format, human readable format is set by default.')
@click.option(
    '-p',
    '--project-id',
    type=Text,
    help='Target project id. It will use environment default if not specified.')
@click.option(
    '-n',
    '--namespace',
    type=Text,
    help='Namespace to use for Kubernetes cluster.all-namespaces is used if not specified.'
)
@click.pass_context
def diagnose_me(ctx: click.Context, json: bool, project_id: str,
                namespace: str):
    """Runs environment diagnostic with specified parameters.

    Feature stage:
    [Alpha](https://github.com/kubeflow/pipelines/blob/07328e5094ac2981d3059314cc848fbb71437a76/docs/release/feature-stages.md#alpha)
    """
    # validate kubectl, gcloud , and gsutil exist
    local_env_gcloud_sdk = gcp.get_gcp_configuration(
        gcp.Commands.GET_GCLOUD_VERSION,
        project_id=project_id,
        human_readable=False)
    for app in ['Google Cloud SDK', 'gsutil', 'kubectl']:
        if app not in local_env_gcloud_sdk.json_output:
            raise RuntimeError(
                '%s is not installed, gcloud, gsutil and kubectl are required '
                % app + 'for this app to run. Please follow instructions at ' +
                'https://cloud.google.com/sdk/install to install the SDK.')

    click.echo('Collecting diagnostic information ...', file=sys.stderr)

    # default behaviour dump all configurations
    results = {}
    for gcp_command in gcp.Commands:
        results[gcp_command] = gcp.get_gcp_configuration(
            gcp_command, project_id=project_id, human_readable=not json)

    for k8_command in k8.Commands:
        results[k8_command] = k8.get_kubectl_configuration(
            k8_command, human_readable=not json)

    for dev_env_command in dev_env.Commands:
        results[dev_env_command] = dev_env.get_dev_env_configuration(
            dev_env_command, human_readable=not json)

    print_to_sdtout(results, not json)


def print_to_sdtout(results: Dict[str, utility.ExecutorResponse],
                    human_readable: bool):
    """Viewer to print the ExecutorResponse results to stdout.

    Args:
      results: A dictionary with key:command names and val: Execution response
      human_readable: Print results in human readable format. If set to True
        command names will be printed as visual delimiters in new lines. If False
        results are printed as a dictionary with command as key.
    """

    output_dict = {}
    human_readable_result = []
    for key, val in results.items():
        if val.has_error:
            output_dict[
                key.
                name] = 'Following error occurred during the diagnoses: %s' % val.stderr
            continue

        output_dict[key.name] = val.json_output
        human_readable_result.append('================ %s ===================' %
                                     (key.name))
        human_readable_result.append(val.parsed_output)

    if human_readable:
        result = '\n'.join(human_readable_result)
    else:
        result = json_library.dumps(
            output_dict, sort_keys=True, indent=2, separators=(',', ': '))

    click.echo(result)
