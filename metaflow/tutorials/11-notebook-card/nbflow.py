from metaflow import step, FlowSpec, Parameter, card, current
from metaflow.cards import Table, Markdown

import functools


def pip(libraries):
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            import subprocess
            import sys

            for library, version in libraries.items():
                print("Pip Install:", library, version)
                subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "pip",
                        "install",
                        "--quiet",
                        library + "==" + version,
                    ]
                )
            return function(*args, **kwargs)

        return wrapper

    return decorator


class NBFlow(FlowSpec):
    """
    you can as follows:
        METAFLOW_DEFAULT_PACKAGE_SUFFIXES='.py,.yaml,.ipynb' python nbflow.py aip run

    Then even use the CLI to get a card
        python nbflow.py --metadata=service --datastore=s3 --no-pylint card get argo-nbflow-xj9x5/start/start-1 --type default > /tmp/default_card.html
        python nbflow.py --metadata=service --datastore=s3 --no-pylint card get argo-nbflow-xj9x5/start/start-1 --type notebook > /tmp/notebook_card.html

    """

    exclude_nb_input = Parameter("exclude_nb_input", default=True, type=bool)

    @pip(libraries={"metaflow-card-notebook": "1.0.7"})
    @card(type="notebook")
    @step
    def start(self):
        self.data_for_notebook = "I Will Print Myself From A Notebook"
        self.nb_options_dict = dict(
            input_path="nbflow.ipynb", exclude_input=self.exclude_nb_input
        )
        self.next(self.hello)

    @step
    def hello(self):
        self.data = "world"
        self.next(self.end)

    @card(type="blank")
    @step
    def end(self):
        current.card.append(Markdown("# Here’s a table"))
        current.card.append(Table([["first", "second"], [1, 2]]))
        print("The End")


if __name__ == "__main__":
    NBFlow()
