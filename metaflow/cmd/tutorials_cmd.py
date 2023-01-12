import os
import shutil

from metaflow._vendor import click

from .util import echo_always, makedirs

echo = echo_always


@click.group()
def cli():
    pass


@cli.group(help="Browse and access the metaflow tutorial episodes.")
def tutorials():
    pass


def get_tutorials_dir():
    metaflow_dir = os.path.dirname(__file__)
    package_dir = os.path.dirname(metaflow_dir)
    tutorials_dir = os.path.join(package_dir, "metaflow", "tutorials")

    if not os.path.exists(tutorials_dir):
        tutorials_dir = os.path.join(package_dir, "tutorials")

    return tutorials_dir


def get_tutorial_metadata(tutorial_path):
    metadata = {}
    with open(os.path.join(tutorial_path, "README.md")) as readme:
        content = readme.read()

    paragraphs = [paragraph.strip() for paragraph in content.split("#") if paragraph]
    metadata["description"] = paragraphs[0].split("**")[1]
    header = paragraphs[0].split("\n")
    header = header[0].split(":")
    metadata["episode"] = header[0].strip()[len("Episode ") :]
    metadata["title"] = header[1].strip()

    for paragraph in paragraphs[1:]:
        if paragraph.startswith("Before playing"):
            lines = "\n".join(paragraph.split("\n")[1:])
            metadata["prereq"] = lines.replace("```", "")

        if paragraph.startswith("Showcasing"):
            lines = "\n".join(paragraph.split("\n")[1:])
            metadata["showcase"] = lines.replace("```", "")

        if paragraph.startswith("To play"):
            lines = "\n".join(paragraph.split("\n")[1:])
            metadata["play"] = lines.replace("```", "")

    return metadata


def get_all_episodes():
    episodes = []
    for name in sorted(os.listdir(get_tutorials_dir())):
        # Skip hidden files (like .gitignore)
        if not name.startswith("."):
            episodes.append(name)
    return episodes


@tutorials.command(help="List the available episodes.")
def list():
    echo("Episodes:", fg="cyan", bold=True)
    for name in get_all_episodes():
        path = os.path.join(get_tutorials_dir(), name)
        metadata = get_tutorial_metadata(path)
        echo("* {0: <20} ".format(metadata["episode"]), fg="cyan", nl=False)
        echo("- {0}".format(metadata["title"]))

    echo("\nTo pull the episodes, type: ")
    echo("metaflow tutorials pull", fg="cyan")


def validate_episode(episode):
    src_dir = os.path.join(get_tutorials_dir(), episode)
    if not os.path.isdir(src_dir):
        raise click.BadArgumentUsage(
            "Episode "
            + click.style('"{0}"'.format(episode), fg="red")
            + " does not exist."
            " To see a list of available episodes, "
            "type:\n" + click.style("metaflow tutorials list", fg="cyan")
        )


def autocomplete_episodes(ctx, args, incomplete):
    return [k for k in get_all_episodes() if incomplete in k]


@tutorials.command(help="Pull episodes " "into your current working directory.")
@click.option(
    "--episode",
    default="",
    help="Optional episode name " "to pull only a single episode.",
)
def pull(episode):
    tutorials_dir = get_tutorials_dir()
    if not episode:
        episodes = get_all_episodes()
    else:
        episodes = [episode]
        # Validate that the list is valid.
        for episode in episodes:
            validate_episode(episode)
    # Create destination `metaflow-tutorials` dir.
    dst_parent = os.path.join(os.getcwd(), "metaflow-tutorials")
    makedirs(dst_parent)

    # Pull specified episodes.
    for episode in episodes:
        dst_dir = os.path.join(dst_parent, episode)
        # Check if episode has already been pulled before.
        if os.path.exists(dst_dir):
            if click.confirm(
                "Episode "
                + click.style('"{0}"'.format(episode), fg="red")
                + " has already been pulled before. Do you wish "
                "to delete the existing version?"
            ):
                shutil.rmtree(dst_dir)
            else:
                continue
        echo("Pulling episode ", nl=False)
        echo('"{0}"'.format(episode), fg="cyan", nl=False)
        # TODO: Is the following redundant?
        echo(" into your current working directory.")
        # Copy from (local) metaflow package dir to current.
        src_dir = os.path.join(tutorials_dir, episode)
        shutil.copytree(src_dir, dst_dir)

    echo("\nTo know more about an episode, type:\n", nl=False)
    echo("metaflow tutorials info [EPISODE]", fg="cyan")


@tutorials.command(help="Find out more about an episode.")
@click.argument("episode", autocompletion=autocomplete_episodes)
def info(episode):
    validate_episode(episode)
    src_dir = os.path.join(get_tutorials_dir(), episode)
    metadata = get_tutorial_metadata(src_dir)
    echo("Synopsis:", fg="cyan", bold=True)
    echo("%s" % metadata["description"])

    echo("\nShowcasing:", fg="cyan", bold=True, nl=True)
    echo("%s" % metadata["showcase"])

    if "prereq" in metadata:
        echo("\nBefore playing:", fg="cyan", bold=True, nl=True)
        echo("%s" % metadata["prereq"])

    echo("\nTo play:", fg="cyan", bold=True)
    echo("%s" % metadata["play"])
