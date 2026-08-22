import json
import os

from metaflow._vendor import click

METAFLOW_ATTACH_CONFIG = {
    "name": "Metaflow: Attach",
    "type": "debugpy",
    "request": "attach",
    "connect": {"host": "localhost", "port": 5678},
    "justMyCode": True,
    "autoAttachChildProcesses": True,
}


@click.group()
def cli():
    pass


@cli.group(help="Commands related to debugging Metaflow flows.")
def debug():
    pass


@debug.group(help="VSCode debugger integration.")
def vscode():
    pass


@vscode.command(
    "install-config",
    help="Install VSCode debug configuration for attaching to Metaflow tasks.",
)
@click.option(
    "--base-port",
    default=5678,
    type=int,
    show_default=True,
    help="Port number for the debugpy attach configuration.",
)
@click.option(
    "--dir",
    "target_dir",
    default=".",
    type=click.Path(),
    help="Workspace root directory where .vscode/ will be created.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing launch.json instead of merging.",
)
@click.option(
    "--remote-root",
    default=None,
    type=str,
    help="Remote container root (e.g. /root/metaflow). Adds pathMappings for remote debugging.",
)
def install_config(base_port, target_dir, overwrite, remote_root):
    target_dir = os.path.abspath(target_dir)
    vscode_dir = os.path.join(target_dir, ".vscode")
    launch_path = os.path.join(vscode_dir, "launch.json")

    our_config = dict(METAFLOW_ATTACH_CONFIG)
    our_config["connect"] = {"host": "localhost", "port": base_port}

    if remote_root is not None:
        import metaflow

        # Parent dir containing the metaflow package (e.g. site-packages or repo root).
        local_metaflow_src = os.path.dirname(os.path.dirname(metaflow.__file__))
        our_config["pathMappings"] = [
            {
                "localRoot": "${workspaceFolder}",
                "remoteRoot": remote_root,
            },
            {
                "localRoot": local_metaflow_src,
                "remoteRoot": remote_root + "/.mf_code",
            },
        ]

    if not os.path.isdir(vscode_dir):
        os.makedirs(vscode_dir)

    if os.path.exists(launch_path) and not overwrite:
        with open(launch_path, "r") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                click.echo(
                    "Warning: existing launch.json is not valid JSON. "
                    "Use --overwrite to replace it.",
                    err=True,
                )
                return

        configs = existing.get("configurations", [])

        # Check if our config already exists
        for i, cfg in enumerate(configs):
            if cfg.get("name") == "Metaflow: Attach":
                if cfg != our_config:
                    configs[i] = our_config
                    existing["configurations"] = configs
                    with open(launch_path, "w") as f:
                        json.dump(existing, f, indent=4)
                        f.write("\n")
                    click.echo("Updated 'Metaflow: Attach' config in %s" % launch_path)
                else:
                    click.echo(
                        "'Metaflow: Attach' config already up to date in %s"
                        % launch_path
                    )
                return

        # Merge: append our config
        configs.append(our_config)
        existing["configurations"] = configs
        with open(launch_path, "w") as f:
            json.dump(existing, f, indent=4)
            f.write("\n")
        click.echo("Added 'Metaflow: Attach' config to existing %s" % launch_path)
    else:
        launch_json = {
            "version": "0.2.0",
            "configurations": [our_config],
        }
        with open(launch_path, "w") as f:
            json.dump(launch_json, f, indent=4)
            f.write("\n")
        click.echo(
            "Created %s with 'Metaflow: Attach' config (port %d)"
            % (launch_path, base_port)
        )
