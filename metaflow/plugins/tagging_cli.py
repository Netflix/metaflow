import sys

from itertools import chain

from metaflow import namespace
from metaflow.client import Flow, Run
from metaflow.util import resolve_identity
from metaflow.exception import CommandException, MetaflowNotFound
from metaflow.exception import MetaflowNamespaceMismatch

from metaflow._vendor import click


def format_tags(obj, system_tags, all_tags, by_tag):
    hdr = [
        "",
        "System tags and *user tags*; only *user tags* can be removed",
        "--------------------------------------------------------",
        "",
    ]
    obj.echo("\n".join(hdr), err=False)
    keys = sorted(set(chain(all_tags.keys(), system_tags.keys())))
    for cat in keys:
        cat_all_tags = all_tags.get(cat, set())
        cat_system_tags = system_tags.get(cat, set())
        if by_tag:
            if cat_all_tags:
                tag_category_format(obj, cat, cat_all_tags, is_system=False)
            if cat_system_tags:
                tag_category_format(obj, cat, cat_system_tags, is_system=True)
        else:
            category_format(
                obj,
                cat,
                cat_system_tags,
                cat_all_tags,
                skip_category=(len(all_tags) == 1),
            )


def category_format(obj, cat, system_tags, all_tags, skip_category):
    if not obj.is_quiet:
        # Pretty printing computations
        max_length = max([len(t) for t in all_tags] if all_tags else [0])

        all_tags = sorted(all_tags)
        if sys.version_info[0] < 3:
            all_tags = [
                t if t in system_tags else "*%s*" % t.encode("utf-8") for t in all_tags
            ]
        else:
            all_tags = [t if t in system_tags else "*%s*" % t for t in all_tags]

        num_tags = len(all_tags)

        # We consider 4 spaces in between column and we consider 120 characters total
        # width and we want 120 >= column_count*max_length + (column_count - 1)*4
        column_count = 124 // (max_length + 4)
        if column_count == 0:
            # Make sure we have at least 1 column even for very very long tags
            column_count = 1
            words_per_column = num_tags
        else:
            words_per_column = (num_tags + column_count - 1) // column_count

        # Extend all_tags by empty tags to be able to easily fill up the lines
        addl_tags = words_per_column * column_count - num_tags
        if addl_tags > 0:
            all_tags.extend([" " for _ in range(addl_tags)])

        lines = []
        if not skip_category:
            lines.append("For run %s" % cat)
        for i in range(words_per_column):
            line_values = [
                all_tags[i + j * words_per_column] for j in range(column_count)
            ]
            length_values = [
                max_length + 2 if l[0] == "*" else max_length for l in line_values
            ]
            formatter = "    ".join(["{:<%d}" % l for l in length_values])
            lines.append(formatter.format(*line_values))
        lines.append("")
        obj.echo("\n".join(lines), err=False)
    else:
        # In quiet mode, we display things much more simply so it is parseable.
        # When displaying by run, we output three columns:
        #  - the comma separated list of runs (usually 1 but more if --all-runs for eg)
        #  - the comma separated list of system tags
        #  - the comma separated list of user tags
        # Columns are separated by a semicolon surrounded by a space on each side
        # to make it visually clear when there are no system tags for example
        obj.echo_always(
            " ; ".join(
                [
                    cat,
                    ",".join(sorted(system_tags)),
                    ",".join(sorted(all_tags.difference(system_tags))),
                ]
            )
        )


def tag_category_format(obj, cat, runs, is_system):
    if not obj.is_quiet:
        # Pretty printing computations
        max_length = max([len(t) for t in runs] if runs else [0])

        all_runs = sorted(runs)

        num_runs = len(all_runs)

        # We consider 4 spaces in between column and we consider 120 characters total
        # width and we want 120 >= column_count*max_length + (column_count - 1)*4
        column_count = 124 // (max_length + 4)
        if column_count == 0:
            # Make sure we have at least 1 column even for very very long tags
            column_count = 1
            words_per_column = num_runs
        else:
            words_per_column = (num_runs + column_count - 1) // column_count

        # Extend all_runs by empty runs to be able to easily fill up the lines
        addl_runs = words_per_column * column_count - num_runs
        if addl_runs > 0:
            all_runs.extend([" " for _ in range(addl_runs)])

        lines = []
        lines.append("For tag %s" % (cat if is_system else "*%s*" % cat))
        for i in range(words_per_column):
            line_values = [
                all_runs[i + j * words_per_column] for j in range(column_count)
            ]
            length_values = [max_length for l in line_values]
            formatter = "    ".join(["{:<%d}" % l for l in length_values])
            lines.append(formatter.format(*line_values))
        lines.append("")
        obj.echo("\n".join(lines), err=False)
    else:
        # In quiet mode, we display things much more simply so it is parseable.
        # When displaying by tag, we output twp columns:
        #  - the tag (user:<tag> or sys:<tag>)
        #  - the comma separated list of runs
        # Columns are separated by a semicolon surrounded by a space on each side
        # to make it consistent with the by_run listing
        obj.echo_always(
            " ; ".join(
                ["system:%s" % cat if is_system else "user:%s" % cat, ",".join(runs)]
            )
        )


def print_tags(obj, metaflow_run_client):
    system_tags = {metaflow_run_client.pathspec: metaflow_run_client.system_tags}
    all_tags = {metaflow_run_client.pathspec: metaflow_run_client.tags}
    return format_tags(obj, system_tags, all_tags, by_tag=False)


def is_prod_token(token):
    return token is not None and token.startswith("production:")


def obtain_metaflow_run_client(obj, run_id, user_namespace):
    if is_prod_token(user_namespace):
        raise CommandException(
            "Modifying a scheduled run's tags is currently not allowed."
        )

    flow_name = obj.flow.name

    # handle error messaging for two cases
    # 1. our user tries to tagging a new flow before it is run
    # 2. our user makes a typo in --namespace
    try:
        namespace(user_namespace)
        Flow(pathspec=flow_name)
    except MetaflowNotFound:
        raise CommandException(
            "Flow *%s* does not exist " % flow_name
            + "in any namespace.\n"
            + "Please run the flow before tagging."
        )
    except MetaflowNamespaceMismatch:
        raise CommandException(
            "Flow *%s* does not belong to " % flow_name
            + "namespace *%s*.\n" % user_namespace
            + "Check typos if you used option --namespace."
        )

    # throw an error with message to include latest run-id when run_id is None
    if run_id is None:
        latest_run_id = Flow(pathspec=flow_name).latest_run.id
        msg = (
            "Please specify a run-id using --run-id\n"
            + "Flow %s's latest run has run-id *%s* under namespace %s"
            % (flow_name, latest_run_id, user_namespace)
        )
        raise CommandException(msg)
    run_id_parts = run_id.split("/")
    if len(run_id_parts) == 1:
        run_name = "%s/%s" % (flow_name, run_id)
    elif len(run_id_parts) == 2:
        if run_id_parts[0] != flow_name:
            raise CommandException(
                "Requesting run-id *%s* which is not a run of flow %s"
                % (run_id, flow_name)
            )
        run_name = run_id
    else:
        raise CommandException("Run-id *%s* is not a valid run-id" % run_id)

    # handle error messaging for three cases
    # 1. our user makes a typo in --run-id
    # 2. our user tries to mutate tags on a meson/production run
    # 3. our user's --run-id does not exist in the default/specified namespace
    try:
        namespace(user_namespace)
        metaflow_client = Run(pathspec=run_name)
    except MetaflowNotFound:
        raise CommandException(
            "Run *%s* does not exist " % run_name
            + "in any namespace.\nPlease check your --run-id."
        )
    except MetaflowNamespaceMismatch:
        namespace(None)
        metaflow_client = Run(pathspec=run_name)

        run_tags = metaflow_client.tags

        if any([is_prod_token(tag) for tag in run_tags]):
            raise CommandException(
                "Modifying a scheduled run's tags is currently not allowed."
            )

        msg = (
            "Run *%s* does not belong to " % run_name
            + "namespace *%s*.\n" % user_namespace
        )

        user_tags = [tag for tag in run_tags if tag.startswith("user:")]
        if user_tags:
            msg += (
                "You can choose any of the following tags associated with *%s* "
                % run_name
                + "to use with option *--namespace*\n"
            )
            msg += "\n".join(["\t*%s*" % tag for tag in user_tags])

        raise CommandException(msg)

    return metaflow_client


def list_runs(obj, user_namespace):
    namespace(user_namespace)
    metaflow_client = Flow(pathspec=obj.flow.name)
    return metaflow_client.runs()


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Metaflow tagging.")
def tag():
    pass


@tag.command("add", help="Add tags to a run.")
@click.option(
    "--run-id",
    required=False,  # set False here so we can throw a better error message
    default=None,
    type=str,
    help="Run ID of the specific run to tag. [required]",
)
@click.option(
    "--namespace",
    "user_namespace",
    required=False,
    default=None,
    type=str,
    help="Change namespace from the default (your username) to " "the specified tag.",
)
@click.argument("tags", required=True, type=str, nargs=-1)
@click.pass_obj
def add(obj, run_id, user_namespace, tags):
    user_namespace = resolve_identity() if user_namespace is None else user_namespace
    metaflow_run_client = obtain_metaflow_run_client(obj, run_id, user_namespace)

    metaflow_run_client.add_tags(tags)

    obj.echo("Operation successful. New tags:", err=False)
    print_tags(obj, metaflow_run_client)


@tag.command("remove", help="Remove tags from a run.")
@click.option(
    "--run-id",
    required=False,  # set False here so we can throw a better error message
    default=None,
    type=str,
    help="Run ID of the specific run to tag.  [required]",
)
@click.option(
    "--namespace",
    "user_namespace",
    required=False,
    default=None,
    type=str,
    help="Change namespace from the default (your username) to " "the specified tag.",
)
@click.argument("tags", required=True, type=str, nargs=-1)
@click.pass_obj
def remove(obj, run_id, user_namespace, tags):
    user_namespace = resolve_identity() if user_namespace is None else user_namespace
    metaflow_run_client = obtain_metaflow_run_client(obj, run_id, user_namespace)

    metaflow_run_client.remove_tags(tags)

    obj.echo("Operation successful. New tags:")
    print_tags(obj, metaflow_run_client)


@tag.command("replace", help="Replace tags of a run.")
@click.option(
    "--run-id",
    required=False,  # set False here so we can throw a better error message
    default=None,
    type=str,
    help="Run ID of the specific run to tag.  [required]",
)
@click.option(
    "--namespace",
    "user_namespace",
    required=False,
    default=None,
    type=str,
    help="Change namespace from the default (your username) to " "the specified tag.",
)
@click.argument("tags", required=True, type=str, nargs=2)
@click.pass_obj
def replace(obj, run_id, user_namespace, tags):
    user_namespace = resolve_identity() if user_namespace is None else user_namespace
    metaflow_run_client = obtain_metaflow_run_client(obj, run_id, user_namespace)

    metaflow_run_client.replace_tag(tags[0], tags[1])

    obj.echo("Operation successful. New tags:")
    print_tags(obj, metaflow_run_client)


@tag.command("list", help="List tags of a run.")
@click.option(
    "--run-id",
    required=False,
    default=None,
    type=str,
    help="Run ID of the specific run to list.",
)
@click.option(
    "--all",
    "list_all",
    required=False,
    is_flag=True,
    default=False,
    help="List tags across all runs of this flow.",
)
@click.option(
    "--my-runs",
    "my_runs",
    required=False,
    is_flag=True,
    default=False,
    help="List tags across all runs of the flow under the default namespaces.",
)
@click.option(
    "--hide-system-tags",
    required=False,
    is_flag=True,
    default=False,
    help="Hide system tags.",
)
@click.option(
    "--by-tag",
    required=False,
    is_flag=True,
    default=False,
    help="Display results by showing runs grouped by tags",
)
@click.option(
    "--by-run",
    required=False,
    is_flag=True,
    default=False,
    help="Display tags grouped by run",
)
@click.argument("arg_run_id", required=False, default=None, type=str)
@click.pass_obj
def tag_list(
    obj, run_id, hide_system_tags, list_all, my_runs, by_tag, by_run, arg_run_id
):
    if run_id is None and arg_run_id is None and not list_all and not my_runs:
        # Assume list_all by default
        list_all = True

    if list_all and my_runs:
        raise CommandException(
            "Option --all cannot be used together with --my-runs.\n"
            + "You can run the flow with commands 'tag list --help' "
            + "to check the meaning of the two options."
        )

    if run_id is not None and arg_run_id is not None:
        raise CommandException(
            "Specify a run either using --run-id or as an argument but not both"
        )

    if arg_run_id is not None:
        run_id = arg_run_id

    if by_run and by_tag:
        raise CommandException("Option --by-tag cannot be used with --by-run")

    system_tags = dict()
    all_tags = dict()

    def add_tags_for_run(run, system_tags, all_tags):
        if by_run:
            if hide_system_tags:
                all_tags[run.pathspec] = run.tags.difference(run.system_tags)
            else:
                system_tags[run.pathspec] = run.system_tags
                all_tags[run.pathspec] = run.tags
        elif by_tag:
            for t in run.tags.difference(run.system_tags):
                all_tags.setdefault(t, []).append(run.pathspec)
            if not hide_system_tags:
                for t in run.system_tags:
                    system_tags.setdefault(t, []).append(run.pathspec)
        else:
            if hide_system_tags:
                all_tags.setdefault("_", set()).update(
                    run.tags.difference(run.system_tags)
                )
            else:
                system_tags.setdefault("_", set()).update(run.system_tags)
                all_tags.setdefault("_", set()).update(run.tags)

    specs = []
    if list_all or my_runs:
        user_namespace = resolve_identity() if my_runs else None
        runs = list_runs(obj, user_namespace)
        for run in runs:
            add_tags_for_run(run, system_tags, all_tags)
            specs.append(run.pathspec)
    else:
        metaflow_run_client = obtain_metaflow_run_client(obj, run_id, None)
        add_tags_for_run(metaflow_run_client, system_tags, all_tags)
        specs.append(metaflow_run_client.pathspec)

    if not by_run and not by_tag:
        # We list all the runs that match to print them out if needed.
        system_tags[",".join(specs)] = system_tags["_"]
        all_tags[",".join(specs)] = all_tags["_"]
        del system_tags["_"]
        del all_tags["_"]

    format_tags(obj, system_tags, all_tags, by_tag)
