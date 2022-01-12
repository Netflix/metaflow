# Metaflow Cards

Metaflow Cards make it possible to produce human-readable report cards automatically from any Metaflow tasks. You can use the feature to observe results of Metaflow runs, visualize models, and share outcomes with non-technical stakeholders.

While Metaflow comes with a built-in default card that shows all outputs of a task without any changes in the code, the most exciting use cases are enabled by custom cards: With a few additional lines of Python code, you can change the structure and the content of the report to highlight data that matters to you. For more flexible or advanced reports, you can create custom card templates that generate arbitrary HTML. 

Anyone can create card templates and share them as standard Python packages. Cards can be accessed via the Metaflow CLI even without an internet connection, making it possible to use them in security-conscious environments. Cards are also integrated with the latest release of the Metaflow GUI, allowing you to enrich the existing task view with application-specific information.

## Technical Details

Metaflow cards can be created by placing an [`@card` decorator](#@card-decorator) over a `@step`. Each decorator takes a `type` argument which defaults to the value `default`. The `type` argument corresponds the [MetaflowCard.type](#metaflowcard). Cards are created after a metaflow task ( instantiation of each `@step` ) completes execution. A separate subprocess gets launched to create a card using the `create` command from the [card_cli](#card-cli). Once the card HTML gets created, it gets stored in the [datastore](#carddatastore). There can be multiple `@card` decorators over a `@step`. 

Since the cards are stored in the datastore we can access them via the `view/get` commands in the [card_cli](#card-cli) or by using the `get_cards` [function](../metaflow/plugins/cards/card_client.py). 

Metaflow ships with a [DefaultCard](#defaultcard) which visualizes artifacts, images, and `pandas.Dataframe`s. Metaflow also ships custom components like `Image`, `Table`, `Markdown` etc. These are subcomponents that are compatible with internal cards. These can be added to a card at `Task` runtime. Cards can also be edited from `@step` code using the [current.card](#editing-metaflowcard-from-@step-code) interface. `current.card` helps add `MetaflowCardComponent`s from `@step` code to a `MetaflowCard`. `current.card` offers methods like `current.card.append` or `current.card['myid']` to helps add components to a card. Since there can be many `@card`s over a `@step`, `@card` also comes with an `id` argument. The `id` argument helps disambigaute the card a component goes to when using `current.card`. For example, setting `@card(id='myid')` and calling `current.card['myid'].append(x)` will append `MetaflowCardComponent` `x` to the card with `id='myid'`.

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
card = card_iterator[0]
card.view()

# Get HTML of card
html =  card_iterator[0].get()
```

### `MetaflowCard`

The [MetaflowCard](../metaflow/plugins/cards/card_modules/card.py) class is the base class to create custom cards. All subclasses require implementing the `render` function. The `render` function is expected to return a string. Below is an example snippet of usage : 
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

`metaflow` exposes default `MetaflowCardComponent`s that can be used with `DefaultCard`. These components can be added at runtime and return a `dict` object on calling their `render` function. The following are the main `MetaflowCardComponent`s available via `metaflow.cards`. 

- `Artifact` : A component to help log artifacts at task runtime. 
    - Example : `Artifact(some_variable,compress=True)`
- `Table` :  A component to create a table in the card HTML. Consists of convenience methods : 
    - `Table.from_dataframe(df)` to make a table from a dataframe.
- `Image` :  A component to create an image in the card HTML. Consists of convenience methods :  
    - `Image(bytearr,"my Image from bytes")`: to directly from `bytes`
    - `Image.from_pil_image(pilimage,"From PIL Image")` : to create an image from a `PIL.Image`
    - `Image.from_matplotlib_plot(plot,"My matplotlib plot")` : to create an image from a plot
- `Error` : A wrapper subcomponent to display errors. Accepts an `exception` and a `title` as arguments. 
- `Section` :  Create a separate subsection with sub components. 
    - Accepts `title`, `subtitle`, `columns:int`, `contents:List[MetaflowCardComponent]` as arguments.
    - Example usage : `Section(contents=[Title("Some new Title"),Artifact([1,2,3,],"Array artifact")])`
- `Title` 
- `Subtitle` 
- `Linechart` [TODO]
- `Barchart` [TODO]
    
### Editing `MetaflowCard` from `@step` code
`MetaflowCard`s can be edited from `@step` code using the `current.card` interface. The `current.card` interface will only be active when a `@card` decorator is placed over a `@step`. To understand the workings of `current.card` consider the following snippet. 
```python
@card(type='blank',id='a')
@card(type='default')
@step
def train(self):
    from metaflow.cards import Markdown
    from metaflow import current
    current.card.append(Markdown('# This is present in the blank card with id "a"'))
    current.card['a'].append(Markdown('# This is present in the default card'))
    self.t = dict(
        hi = 1,
        hello = 2
    )
    self.next(self.end)
```
In the above scenario there are two `@card` decorators which are being customized by `current.card`. The `current.card.append`/ `current.card['a'].append` methods only accepts objects which are subclasses of `MetaflowCardComponent`. The `current.card.append`/ `current.card['a'].append` methods only add a component to **one** card. Since there can be many cards for a `@step`, a **default editabled card** is resolved to disambiguate which card has access to the `append`/`extend` methods within the `@step`. A default editable card is a card that will have access to the `current.card.append`/`current.card.extend` methods. `current.card` resolve the default editable card before a `@step` code gets executed. It sets the default editable card once the last `@card` decorator calls the `task_pre_step` callback. In the above case, `current.card.append` will add a `Markdown` component to the card of type `default`. `current.card['a'].append` will add the `Markdown` to the `blank` card whose `id` is `a`. A `MetaflowCard` can be user editable, if `ALLOW_USER_COMPONENTS` is set to `True`. Since cards can be of many types, **some cards can also be non editable by users** (Cards with `ALLOW_USER_COMPONENTS=False`). Those cards won't be eligible to access the `current.card.append`. A non user editable card can be edited through expicitly setting an `id` and accessing it via `current.card['myid'].append` or by looking it up by its type via `current.card.get(type=’pytorch’)`.

#### `current.card` (`CardComponentCollector`)

The `CardComponentCollector` is the object responsible for resolving a `MetaflowCardComponent` to the card referenced in the `@card` decorator. 

Since there can be many cards,  `CardComponentCollector` has a `_finalize` function. The `_finalize` function is called once the **last** `@card` decorator calls `task_pre_step`. The `_finalize` function will try to find the **default editable card** from all the `@card` decorators on the `@step`. The default editable card is the card that can access the `current.card.append`/`current.card.extend` methods. If there are multiple editable cards with no `id` then `current.card` will throw warnings when users call `current.card.append`. This is done because `current.card` cannot resolve which card the component belongs.  

The `@card` decorator also exposes another argument called `customize=True`. **Only one `@card` decorator over a `@step` can have `customize=True`**. Since cards can also be added from CLI when running a flow, adding `@card(customize=True)` will set **that particular card** from the decorator as default editable. This means that `current.card.append` will append to the card belonging to `@card` with `customize=True`. If there is more than one decorator with `customize=True` we throw warnings that `current.card.append` won't append to any card. 

One important feature of the `current.card` object is that it will not fail.  Even when users try to access `current.card.append` with multiple editable cards, we throw warnings but don't fail. `current.card` will also not fail when a user tries to access a card of a non-existing id via `current.card['mycard']`. Since `current.card['mycard']` gives reference to a `list` of `MetaflowCardComponent`s, `current.card` will return a non-referenced `list` when users try to access the dictionary inteface with a non existing id (`current.card['my_non_existant_card']`). 

Once the `@step` completes execution, every `@card` decorator will call `current.card._serialize` (`CardComponentCollector._serialize`) to get a JSON serializable list of `str`/`dict` objects. The `_serialize` function internally calls all [component's](#metaflowcardcomponent) `render` function. This list is `json.dump`ed to a `tempfile` and passed to the `card create` subprocess where the `MetaflowCard` can use them in the final output. 

### Creating Custom Cards 
Custom cards can be installed using the `metaflow_extensions` submodule. 
```
your_package/ # the name of this dir doesn't matter
├ setup.py
├ metaflow_extensions/ 
│  └ organizationA/      # package to keep org level extensions seperate ; # dir name must match the package name in `setup.py`
│      ├__init__.py    # This will have an __init__.py file
│      └ plugins/ # NO __init__.py file
│        └ cards/ # NO __init__.py file # This is a namespace package. 
│           └ my_card_module/  # Name of card_module
│               └ __init__.py. # This is the __init__.py is required to recoginize `my_card_module` as a package
│               └ somerandomfile.py. # Some file as a part of the package. 
.
```

The `__init__.py` of the `metaflow_extensions.organizationA.plugins.cards.my_card_module`, requires a `CARDS` attribute which needs to be a `list` of objects inheriting `MetaflowCard` class. For Example, in the below `__init__.py` file exposes a `MetaflowCard` of `type` "y_card2". 

```python
from metaflow.cards import MetaflowCard

class YCard(MetaflowCard):
    type = "y_card2"

    ALLOW_USER_COMPONENTS = True

    def __init__(self, options={}, components=[], graph=None):
        self._components = components

    def render(self, task):
        return "I am Y card %s" % '\n'.join([comp for comp in self._components])

CARDS = [YCard]
```

Having this `metaflow_extensions` module present in the PYTHONPATH can also work. Custom cards can also be created by reusing components provided by metaflow. For Example : 
```python
from metaflow.cards import BlankCard
from metaflow.cards import Artifact,Table,Section

class MyCustomCard(BlankCard):

    type = 'my_custom_card'
    
    def render(self, task):
        art_com = Section(title='My Section Added From Card',
            contents=[
                Table(
                    [[Artifact(k.data,k.id)] for k in task]
                )
            ]
        ).render()
        return super().render(task,components=[art_com])

CARDS = [MyCustomCard]
```
## TODOS

- [x] `@card` decorator
- [x] `CardDatastore`
- [x] Card CLI 
- [x] `get_cards` (Metaflow client for cards)
- [x] `MetaflowCard` (Core class to implement custom cards)
- [x] `MetaflowCardComponent` (Subcomponents appendable to `MetaflowCard`s during the runtime of a `Flow`)
- [x]  Default `MetaflowCardComponent`s
- [x] `DefaultCard` (A default `MetaflowCard`) 
- [x] Adding Components to cards in runtime. 
- [ ] Creating custom instable Metaflow cards