import os
import json
import shutil
from collections import defaultdict
from metaflow._vendor import click
import metaflow.tracing as tracing
from metaflow.plugins.pypi import MAGIC_FILE
from metaflow.plugins.pypi.micromamba import Micromamba
from metaflow.plugins.datastores.local_storage import LocalStorage

micromamba = Micromamba()


class DefaultDictEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, defaultdict):
            return dict(obj)
        return json.JSONEncoder.default(self, obj)


def show_tabular_output(envs_data, all_envs=False):
    headers = ["ID", "Path", "Step", "Platform", "Type"]

    # Display headers
    print("{:<18} {:<67} {:<8} {:<12} {:<12}".format(*headers))

    # Display data
    for item in envs_data:
        if all_envs or item["step"] is not None:
            row = [
                item["id"],
                item["path"],
                item["step"] if item["step"] is not None else "",
                ", ".join(item["platform"]),
                ", ".join(item["type"]),
            ]
            print("{:<18} {:<67} {:<8} {:<12} {:<12}".format(*row))


@click.group()
def cli():
    pass


@cli.group(help="Commands related to conda.")
def conda():
    pass


@tracing.cli_entrypoint("conda/list")
@conda.command(name="list", help="List conda environments for a flow.")
@click.option(
    "-a",
    "--show-all",
    is_flag=True,
    default=False,
    help="Show stale environments as well for a flow.",
)
@click.option(
    "-j", "--jsonify", is_flag=True, default=False, help="Show output in JSON format."
)
@click.pass_context
def list_envs(ctx, show_all, jsonify):
    manifest_path = os.path.join(
        LocalStorage.get_datastore_root_from_config(ctx.obj.echo),
        ctx.obj.flow.name,
        MAGIC_FILE,
    )
    if not os.path.exists(manifest_path):
        print(f"No conda environments for the flow: {ctx.obj.flow.name}")
    else:
        list_envs_output = []

        env_id_to_step_name = {}
        conda_env = ctx.obj.environment
        for each_step in ctx.obj.flow:
            env_id = conda_env.get_environment(each_step).get("id_")
            env_id_to_step_name[env_id] = each_step.name

        with open(manifest_path, "r") as f:
            env_data = json.load(f)
            for env_id, platforms_data in env_data.items():
                each_env = defaultdict(list)
                each_env["id"] = env_id
                each_env["path"] = micromamba.path_to_environment(env_id)
                each_env["step"] = (
                    env_id_to_step_name[env_id]
                    if env_id in env_id_to_step_name
                    else None
                )
                for platform, types_data in platforms_data.items():
                    each_env["platform"].append(platform)
                    for type_, _ in types_data.items():
                        each_env["type"].append(type_)
                list_envs_output.append(each_env)

        if jsonify and show_all:
            raise ValueError(
                "Supply one of ['-j'/'--jsonify', '-a'/'--show-all'], not both"
            )

        if jsonify:
            json_data = json.dumps(list_envs_output, cls=DefaultDictEncoder, indent=4)
            print(json_data)
        else:
            show_tabular_output(list_envs_output, show_all)


@tracing.cli_entrypoint("conda/clean")
@conda.command(name="clean", help="Remove conda environments associated with a flow.")
@click.option(
    "-m",
    "--manifest",
    is_flag=True,
    default=False,
    help="Delete the 'conda.manifest' file as well.",
)
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    default=False,
    help="Skip confirmations for deleting environments.",
)
@click.pass_context
def remove_envs_and_delete_files(ctx, manifest, yes):
    manifest_path = os.path.join(
        LocalStorage.get_datastore_root_from_config(ctx.obj.echo),
        ctx.obj.flow.name,
        MAGIC_FILE,
    )
    if not os.path.exists(manifest_path):
        print(f"No conda environments for the flow: {ctx.obj.flow.name}")
    else:
        with open(manifest_path, "r") as f:
            env_data = json.load(f)
            for env_id, _ in env_data.items():
                path = micromamba.path_to_environment(env_id)
                if os.path.exists(path):
                    if yes:
                        click.echo(f"Deleting the conda environment at {path}")
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        confirm = click.prompt(
                            f"Do you want to delete the conda environment at {path}?",
                            type=click.Choice(["y", "n"]),
                        )
                        if confirm == "y":
                            click.echo(f"Deleting the conda environment at {path}")
                            shutil.rmtree(path, ignore_errors=True)
                        else:
                            click.echo(
                                f"Skipping the deletion of conda environment at {path}"
                            )

        if manifest:
            click.echo(f"Deleting the 'conda.manifest' file at: {manifest_path}")
            os.remove(manifest_path)
