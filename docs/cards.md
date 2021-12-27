# Metaflow Cards

Metaflow Cards make it possible to produce human-readable report cards automatically from any Metaflow tasks. You can use the feature to observe results of Metaflow runs, visualize models, and share outcomes with non-technical stakeholders.

While Metaflow comes with a built-in default card that shows all outputs of a task without any changes in the code, most exciting use cases are enabled by custom cards: With a few additional lines of Python code, you can change the structure and the content of the report to highlight data that matters to you. For more flexible or advanced reports, you can create custom card templates that generate arbitrary HTML. 

Anyone can create card templates and share them as standard Python packages. Cards can be accessed via the Metaflow CLI even without an internet connection, making it possible to use them in security-conscious environments. Cards are also integrated with the latest release of the Metaflow GUI, allowing you to enrich the existing task view with application-specific information.

## Key Components

Metaflow cards can be created by placing an [`@card`](#@card-decorator) [decorator](../metaflow/plugins/cards/card_decorator.py) over a `@step`. Each decorator takes a `type` argument which defaults to the value `default`. The `type` argument corresponds the `MetaflowCard.type`. Cards are created after a Metaflow Task ( instantiation of each `@step` ) completes execution. A separate subprocess gets launched to create a card using the `create` command from the [card_cli](#card-cli). Once the card HTML gets created, it gets stored in the datastore. 

Since the cards are stored in the datastore we can access them via the `view/get` methods in the [card_cli](../metaflow/plugins/cards/card_cli.py) or using the `get_cards` [function](../metaflow/plugins/cards/card_client.py) which accepts a `Task` object or a `pathspec` string. 

`metaflow` ships with a `DefaultCard` which visualizes artifacts, images, and `pandas.Dataframe`s. `metaflow` also ships custom components like `Barchart`s or `Linechart`s or `Image`s or `Table`s. Components can be added to a card at `Task` runtime such that they show up in the generated card. These components are only compatible with the `DefaultCard` card class.

### `@card` decorator
The `@card` [decorator](../metaflow/plugins/cards/card_decorator.py) is implemented by inheriting the `StepDecorator`. The decorator can be placed over `@step` to create an HTML file visualizing information from the task.

#### Parameters
- `type` `(str)` [Defaults to `default`]: The `type` of `MetaflowCard` to create. More details on `MetaflowCard`s is provided [later in this document](#metaflowcard). 
- `options` `(dict)` : options to instantiate a `MetaflowCard`. `MetaflowCard`s will be instantiated with the `options` keyword argument. The value of this argument is a dictionary. 
- `timeout` `(int)` [Defaults to `45`]: Amount of time to wait before killing the card subprocess and 
- `save_errors` `(bool)` [Defaults to `True`]: If set to `True` then the failure in rendering any `MetaflowCard` will generate `ErrorCard` instead with the full stack trace of the failure. 

#### Usage Semantics

```python
from metaflow import FlowSpec,step,batch,current,Parameter,card

class ModelTrainingFlow(FlowSpec):
    num_rows = Parameter('num-rows',default = 1000000,type=int,help='The number of rows from the dataset to use for Training.')

    batch_size = Parameter('batch-size',default = 64,type=int,help='Batch size to use for training the model.')

    max_epochs = Parameter(
        'max-epochs',\
        envvar="MAX_EPOCHS",\
        default=1,type=int,help='Maximum number of epochs to train model.'
    )

    num_gpus = Parameter(
        'num-gpus',
        envvar="NUM_GPUS",\
        default=0,type=int,help='Number of GPUs to use when training the model.'
    )

    @step
    def start(self):
        self.next(self.train)

    @card(
        type='default',
        options={"only_repr":False},
        timeout=100,
        save_errors = False
    )
    @step
    def train(self):
        import random
        import numpy as np
        self.loss = np.random.randn(100,100)*100
        self.next(self.end)
    
    @step
    def end(self):
        print("Done Computation")

if __name__ == "__main__":
    ModelTrainingFlow()
```



### `CardDatastore`
The [CardDatastore](../metaflow/plugins/cards/card_datastore.py) is used by the the [card_cli](#card-cli) and the [metaflow card client](#access-cards-in-notebooks) (`get_cards`). It exposes methods to get metadata about a card and the paths to cards for a `pathspec`. 

### Card CLI
Methods exposed by the [card_cli](../metaflow/plugins/cards/.card_cli.py). :

- `create` : Creates the card in the datastore for a `Task`. Adding a `--render-error-card` will render a `ErrorCard` upon failure to render the card of the selected `type`. If `--render-error-card` is not passed then the CLI will fail loudly with the exception. 
```sh
# python myflow.py card create <pathspec> --type <type_of_card> --timeout <timeout_for_card> --options "{}"  
python myflow.py card create 100/stepname/1000 --type default --timeout 10 --options '{"only_repr":false}' --render-error-card
```

- `view/get` : Calling the `view` CLI method will open the card associated for the pathspec in a browser. The `get` method gets the HTML for the card and prints it. You can call the command in the following way. Adding `--follow-resumed` as argument will retrieve the card for the origin resumed task. 
```sh
# python myflow.py card view <pathspec> --hash <hash_of_card> --type <type_of_card> 
python myflow.py card view 100/stepname/1000 --hash ads34 --type default --follow-resumed 
```

### Access cards in notebooks
Metaflow also exposes a `get_cards` client that helps resolve cards outside the CLI. Example usage is shown below : 
```python
from metaflow import Task
from metaflow.cards import get_cards

taskspec = 'MyFlow/1000/stepname/100'
task = Task(taskspec)
card_iterator = get_cards(task) # you can even call `get_cards(taskspec)`

# view card in browser
card_iterator[0].view()

# Get HTML of card
html =  card_iterator[0].get()
```

### `MetaflowCard`

The [MetaflowCard](../metaflow/plugins/cards/card_modules/card.py) class is the base class from which all custom cards are created. The class provides a `render` function. The `render` function returns a string. Below is an example snippet of usage : 
```python
from metaflow.cards import MetaflowCard
# path to the custom html file which is a `mustache` template.
PATH_TO_CUSTOM_HTML = 'myhtml.html'

class CustomCard(MetaflowCard):
    type = "custom_card"

    def __init__(self, options={"no_header": True}, graph=None,components=[]):
        super().__init__()
        self._no_header = True
        self._graph = graph
        if "no_header" in options:
            self._no_header = options["no_header"]

    def render(self, task):
        pt = self._get_mustache()
        data = dict(
            graph = self._graph,
            header = self._no_header
        )
        html_template = None
        with open(PATH_TO_CUSTOM_HTML) as f:
            html_template = f.read()
        return pt.render(html_template,data)
```

The class consists of the `_get_mustache` method that returns [chevron](https://github.com/noahmorrison/chevron) object ( a `mustache` based [templating engine](http://mustache.github.io/mustache.5.html) ). Using the `mustache` templating engine you can rewrite HTML template file. In the above example the `PATH_TO_CUSTOM_HTML` is the file that holds the `mustache` HTML template. 

#### Parameters
- `components` `(List[MetaflowCardComponent])`: List of `MetaflowCardComponent` added at `Flow` runtime.
- `graph` `(Dict[str,dict])`: The DAG associated to the flow. It is a dictionary of the form `stepname:step_attributes`. `step_attributes` is a dictionary of metadata about a step , `stepname` is the name of the step in the DAG.  
- `options` `(dict)`: helps control the behavior of individual cards. 
    - For example, the `DefaultCard` supports `options` as dictionary of the form `{"only_repr":True}`. Here setting `only_repr` as `True` will ensure that all artifacts are serialized with `reprlib.repr` function instead of native object serialization. 


### `MetaflowCardComponent`
`MetaflowCard` class accepts `components` as a keyword arguement. `components` is a list of  `render`ed `MetaflowCardComponent`.

The `render` function of the `MetaflowCardComponent` class returns a `string` or `dict`. It can be called in the `MetaflowCard` class or passed during runtime execution. An example of using `MetaflowCardComponent` inside `MetaflowCard` can be seen below : 
```python
from metaflow.cards import MetaflowCard,MetaflowCardComponent

class Title(MetaflowCardComponent):
    def __init__(self,text):
        self._text = text

    def render(self):
        return "<h1>%s</h1>"%self._text

class Text(MetaflowCardComponent):
    def __init__(self,text):
        self._text = text

    def render(self):
        return "<p>%s</p>"%self._text

class CustomCard(MetaflowCard):
    type = "custom_card"

    HTML = "<html><head></head><body>{data}<body></html>"

    def __init__(self, options={"no_header": True}, graph=None,components=[]):
        super().__init__()
        self._no_header = True
        self._graph = graph
        if "no_header" in options:
            self._no_header = options["no_header"]

    def render(self, task):
        pt = self._get_mustache()
        data = '\n'.join([
            Title("Title 1").render(),
            Text("some text comes here").render(),
            Title("Title 2").render(),
            Text("some text comes here again").render(),
        ])
        data = dict(
            data = data
        )
        html_template = self.HTML
        
        return pt.render(html_template,data)
```

### `DefaultCard`
The [DefaultCard](../metaflow/plugins/cards/card_modules/basic.py) is a default card exposed by metaflow. This will be used when the `@card` decorator is called without any `type` argument or called with `type='default'` argument. It will also be the default card used with cli. The card uses a [HTML template](../metaflow/plugins/cards/card_modules/base.html) along with these [JS](../metaflow/plugins/cards/card_modules/main.js) and [CSS](../metaflow/plugins/cards/card_modules/bundle.css) files. 

The [HTML](../metaflow/plugins/cards/card_modules/base.html) is a template which works with [JS](../metaflow/plugins/cards/card_modules/main.js) and [CSS](../metaflow/plugins/cards/card_modules/bundle.css). 

The JS and CSS are created after building the JS and CSS from the [cards-ui](../metaflow/plugins/cards/ui/README.md) directory. [cards-ui](../metaflow/plugins/cards/ui/README.md) consists of the JS app that generates the HTML view from a JSON object. 

### Default `MetaflowCardComponent`

`metaflow` exposes default `MetaflowCardComponent`s that can be used with `DefaultCard`. These components can be added at runtime and return a `dict` object on calling their `render` function. The following are the main `MetaflowCardComponent`s which are can be imported from `metaflow.cards`. 

- `Title` 
- `Subtitle` 
- `Artifact` : A component to help log artifacts at task runtime. 
    - Example : `Artifact(some_variable,compress=True)`
- `Table` :  A component to create a table in the card HTML. Consists of convenience methods : 
    - `Table.from_dataframe(df,"my table heading")` to make a table from a dataframe.
- `Image` :  A component to create an image in the card HTML. Consists of convenience methods :  
    - `Image.from_bytes(bytearr,"my Image from bytes")`: to directly from `bytes`
    - `Image.from_pil_image(pilimage,"From PIL Image")` : to create an image from a `PIL.Image`
    - `Image.from_matplotlib_plot(plot,"My matplotlib plot")` : to create an image from a plot
- `Error` : A wrapper subcomponent to display errors. Accepts an `exception` and a `title` as arguments. 
- `Section` :  Create a separate subsection with sub components. 
    - Accepts `title`, `subtitle`, `columns:int`, `contents:List[MetaflowCardComponent]` as arguments.
    - Example usage : `Section(contents=[Title("Some new Title"),Artifact([1,2,3,],"Array artifact")])`
- `Linechart` [TODO]
- `Barchart` [TODO]
    
## TODOS

- [x] `@card` decorator
- [x] `CardDatastore`
- [x] Card CLI 
- [x] `get_cards` (Metaflow client for cards)
- [x] `MetaflowCard` (Core class to implement custom cards)
- [x] `MetaflowCardComponent` (Subcomponents appendable to `MetaflowCard`s during the runtime of a `Flow`)
- [x]  Default `MetaflowCardComponent`s
- [x] `DefaultCard` (A default `MetaflowCard`) 
- [ ] Adding Components to cards in runtime. 
- [ ] Creating custom instable Metaflow cards