import json
import os

from metaflow._vendor import click


@click.group()
def cli():
    pass


@cli.command()
@click.option("--output_file")
def parameters(output_file: str):
    input = json.loads(os.environ.get("METAFLOW_PARAMETERS", "{}"))
    params = json.loads(os.environ.get("METAFLOW_DEFAULT_PARAMETERS", "{}"))
    params.update(input)
    with open(output_file, "w") as f:
        for k in params:
            # Replace `-` with `_` is parameter names since `-` isn't an
            # allowed character for environment variables. cli.py will
            # correctly translate the replaced `-`s.
            normalized_name = k.upper().replace("-", "_")
            dumps = json.dumps(params[k])
            value = f"'{dumps}'" if isinstance(params[k], dict) else dumps
            f.write(f"export METAFLOW_INIT_{normalized_name}={value}\n")
    os.chmod(output_file, 509)


if __name__ == "__main__":
    cli()
