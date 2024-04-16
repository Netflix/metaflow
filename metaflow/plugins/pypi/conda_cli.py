import os
import json
import shutil
from metaflow._vendor import click
import metaflow.tracing as tracing
from metaflow.plugins.pypi import MAGIC_FILE
from metaflow.plugins.pypi.micromamba import Micromamba

micromamba = Micromamba()


def extract_from_manifest(data_from_manifest):
    env_ids = []
    platforms = []
    types = []
    paths = []

    for env_id, platforms_data in data_from_manifest.items():
        for platform, types_data in platforms_data.items():
            for type_, _ in types_data.items():
                env_ids.append(env_id)
                platforms.append(platform)
                types.append(type_)
                paths.append(micromamba.path_to_environment(env_id))

    return env_ids, platforms, types, paths


@click.group()
def cli():
    pass


@cli.group(help="Commands related to conda.")
def conda():
    pass


@tracing.cli_entrypoint("conda/list")
@conda.command(name="list", help="List conda environments.")
@click.pass_context
def list_envs(ctx):
    # TODO get step information
    manifest_path = os.path.join(
        ctx.obj.flow_datastore.datastore_root, ctx.obj.flow.name, MAGIC_FILE
    )
    if not os.path.exists(manifest_path):
        print(f"No conda environments for the flow: {ctx.obj.flow.name}")
    else:
        with open(manifest_path, "r") as f:
            env_data = json.load(f)
            print(
                "{:<18} {:<12} {:<10} {:<10}".format(
                    "Environment ID", "Platform", "Type", "Environment Path"
                )
            )
            env_ids, platforms, types, paths = extract_from_manifest(env_data)
            for _id, platform, _type, path in zip(env_ids, platforms, types, paths):
                print("{:<18} {:<12} {:<10} {:<10}".format(_id, platform, _type, path))


@tracing.cli_entrypoint("conda/clean")
@conda.command(name="clean", help="Remove conda environments and ")
@click.pass_context
def remove_envs_and_delete_files(ctx):
    # TODO: where are lock files located?
    manifest_path = os.path.join(
        ctx.obj.flow_datastore.datastore_root, ctx.obj.flow.name, MAGIC_FILE
    )
    if not os.path.exists(manifest_path):
        print(f"No conda environments for the flow: {ctx.obj.flow.name}")
    else:
        with open(manifest_path, "r") as f:
            env_data = json.load(f)
            env_ids, _, _, paths = extract_from_manifest(env_data)

            for _id, path in zip(env_ids, paths):
                if os.path.exists(path):
                    print(f"Removing conda environment with id: {_id} at path: {path}")
                    shutil.rmtree(path, ignore_errors=True)

        print(f"Deleting the 'conda.manifest' file at: {manifest_path}")
        os.remove(manifest_path)
