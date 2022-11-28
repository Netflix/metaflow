# Environment escape design
## Motivation

To best control dependencies for a Metaflow run, Metaflow provides Conda which
allows users to define and "pin" the environment their flow executes in. This
prevents packages from shifting from under the user and guarantees that the
environment that Metaflow runs in is the same every time. This is similar to
the guarantees provided by using a Docker container but makes it easier for the
user as there is no need to bake an image every time.

In some cases, however, this is not ideal. Certain packages may not exist in
Conda or, more importantly, you may need certain packages that need to shift
from under you (particularly packages that may interface with other systems like
a package to access data). The environment escape plugin allows Metaflow to
support this model where *most* code executes in a pinned environment like Conda
but *some* can execute in another Python environment.

## High-level design

At a high-level, the environment escape plugin allows a Python interpreter to
forward calls to another interpreter. To set semantics, we will say that a
*client* interpreter escapes to a *server* interpreter. The *server* interpreter
operates in a slave-like mode with regard to the *client*. To give a concrete
example, imagine a package ``data_accessor`` that is available in the base
environment you are executing in but not in your Conda environment. When
executing within the Conda environment, the *client* interpreter is the Conda
Python interpreter operating within the confines of the Conda environment; it
**escapes** to the *server* interpreter which is the Python interpreter present
in the base environment and in which ``data_accessor`` is accessible. From a
user's point-of-view, the ```data_accessor``` package can be imported as usual
within the *client* environment; under the hood, however, any computation
happening as part of that module actually goes through the environment escape
plugin and is executed by the *server* interpreter.

To illustrate this high level-design, let us walk through an example. Suppose
the user code is as follows:
```
import data_accessor as da

sql = 'select * from %s order by int' % name.replace('/', '.')
job = da.SqlJob()\
        .script(sql)\
        .headers()\
        .execute()

job.wait()
job.raise_for_status()
result = job.pandas().to_dict()
```

In the above snippet ```SqlJob()``` creates an object that cannot exist as is on
the client side since ```data_accessor``` does not exist. Instead, a *stub
object* will stand in on the client side for the ```data_accessor``` object on
the server side. All methods (here ```script```, ```wait``` and
```raise_for_status``` for example) will be forwarded by the stub to be executed
on the server side.

Digging a little deeper, the code first uses a builder pattern whereby each
method returns ```self```. For example, ```script```, ```headers``` and
```execute``` all return a modified version of the same object. When the client
wants to execute the ```script``` method for example, it will encode the
identifier of the stub object as well as the method name (along with any
arguments) and send it to the server. The server will then decode the
identifier, use it to map the stub object making the call to its local object
and proceed to use that object to call the method on it. When returning, the
server will send back to the client an identifier for the object. In this case,
it will be the same object so the same identifier. The client will then use that
identifier to find the correct stub. There is therefore a **one-to-one mapping
between stub objects on the client and backing objects on the server**.

The next method called on ```job``` is ```wait``` which returns ```None```. In
this system, by design, only certain objects may be transferred between
the client and the server:
- any Python basic type; this can be extended to any object that can be pickled
  without any external library;
- any reference to a server object provided that object is exportable (more on
  this later)
- any container containing a combination of the above two types (lists, sets,
  tuples, dictionaries)

The next method, ```raise_for_status``` can potentially raise an exception. The
environment escape plugin will rethrow all exceptions thrown on the server to
the client. The plugin will make a best-effort to recreate the exception on the
client side. Exceptions that exist on the client (for example all the standard
exceptions) will be re-thrown that way (in other words, an ```AttributeError```
in the server will cause an ```AttributeError``` to be thrown in the client);
exceptions that do not exist will be created on the fly and inherit from
```RemoteInterpreterException``` and contain best-effort representations of all
the attributes of the original exception (either the attribute itself if it can
be transferred or a string representation of it).

### Key Concepts
There are a few key decisions in the implementation that stem from the principle
of "let there be no surprises":
- The environment escape plugin is *whitelist* based. By default, the server
  cannot transfer *any* objects back to the client (this is rather useless).
  Classes need to be explicitly whitelisted when defining a module to be used
  with the plugin. Any object that needs to be sent from the server back to the
  client that is not whitelisted will cause an error. Note that whitelisting a
  base class will **not** allow all of its children classes to be sent back; the
  library uses ```type()``` to determine the type of an object to send back and
  that object must be explicitly whitelisted for the object to be sent through.
  - Additional objects may be specified as well that do not belong to the
    library being emulated. For example, ```data_accessor``` functions may
    return a ```functools.partial``` object. The emulated library can also
    whitelist any other object that would be available on both the client and
    server as things that are allowed to be sent through the environment escape
    plugin. It is recommended to stick with the Python standard library to limit
    compatibility issues.
  - Exceptions are always rethrown to the client. The server will never die when
    catching an exception to allow the client to decide how best to proceed.
  - The environment escape plugin allows for the definition of *overrides* that
    can intercept any method call both on the client prior to forwarding the
    request to the server and on the server prior to executing the method on the
    local object. This allows for the customization of communication in
    particular.

### Credit

A big part of the design was inspired by an OpenSource project called RPyC
although the implementation was totally re-written and simplified due to the
restrictions/constraints we imposed. Information about this project can be found
here: https://rpyc.readthedocs.io/en/latest/.

## Implementation details

### Communication
Communication is quite simple in this implementation and relies on UNIX Sockets
(defined in ```communication/socket_bytestream.py```). The methods exposed by
this level are very simple:
- read a fixed number of bytes (this imposes length-encoded messages but makes
  communication that much simpler)
- send data in a buffer; all data is sent (although this may be over several
  tries)

Above the socket, a ```channel``` sends and receives messages. It uses JSON to
serialize and deserialize messages (which are effectively very simple
dictionaries).

Finally, above that, ```data_transferer.py``` is responsible for encoding and
decoding the messages that are sent. To encode, it takes regular Python objects
and produces a JSON-able object (typically a dictionary with string keys and
jsonable objects as values). The decoding is the reverse where a dictionary is
taken from the channel and Python objects are returned.

Transferring exceptions requires a tiny bit more work and this logic can be found
in ```exception_transferer.py```; this relies on ```data_transferer.py``` to do
the actual encoding and decoding and ```exception_transferer.py``` merely takes
care of the specificities of extracting the information needed from the
exception to re-create it on the other side.

### Stub objects

The crux of the work happens in ```stub.py``` which describes what a stub class
looks like on the client side.

#### Creation

Each class on the server side will get a corresponding stub class (so not all
stubs are the same class, they just look very similar). This is handled in
```create_class``` which does the following:
- it gathers all the methods from the class (this is obtained from the server --
  see Section on the Client) and creates local methods for the stub class that
  it is building. It distinguishes regular methods, static methods and class
  methods.
- ```create_class``` also handles client overrides at this stage. If a method
  has an override present, the method created will point to the override. If no
  override is present, the method created basically forwards the call to the
  server via the ```fwd_request``` call.

We use a specific MetaClass ```MetaWithConnection```, the use of which is
detailed directly in the source file. The basic idea is to be able to handle the
creation of stub objects both locally on the client (where the client does
```Table('foobar')``` expecting the object to be created on the server and a
stub to be returned) as well as remotely when the server returns a created
object.

#### Content of a stub object

Stub objects really do not have much locally; they forward pretty much
everything to the server:
- Attributes are all forwarded to the server (minus very few) via
  ```__getattribute__``` and ```__getattr__```.
  - Methods are inserted using the previously described mechanism.
  - Special methods are also typically handled by forwarding the request to the
    server.

  Stub objects do contain certain important elements:
  - a reference to the client to use to forward request
  - an identifier that the server can use to link the stub object to its local
    object
  - the name of the class
  - (TODO): There is a refcount but that doesn't seem to be fully working yet --
    the idea was to make sure the server object stayed alive only as long as the
    client object.

#### Method invocation on a stub object

  When invoking a method on a stub object, the following happens:
  - if a local override is defined, the local override is called and is passed:
  - the stub on which the method is called
  - a function object to call to forward the method to the server. This function
    object requires the arguments to be passed to it (so you can modify them)
    but nothing else. It is a standalone function object and does not need to be
    called as a method of the stub.
  - the initial arguments and keyword arguments passed to the call
  - if a local override is not defined, the call is forwarded to the server
    using the arguments and keyword arguments passed in.
  - on the server side, if a remote override is defined, the remote override is
    called and is passed:
  - the object on which the method is being called
  - a function object to call to forward the method to the object. This function
    object requires the arguments to be passed to it (so you can modify them)
    but nothing else. It is a standalone function object and already bound to
    the object.
  - the arguments and keyword arguments received from the client
  - if a remote override is not defined, the method is called directly on the
    object.

### Client/Server

  The directionality imposed by the design is intentional (although not strictly
  required): the client is where user-code originates and the server only
  performs computations at the request of the client when the client is unable
  to do so.

  The server is thus started by the client, and the client is responsible for
  terminating the server when it dies. A big part of the client and server code 
  consist in loading the configuration for the emulated module, particularly the
  overrides.

  The steps to bringing up the client/server connection are as follows:
  - [Client] Determines a path to the UNIX socket to use (a combination of PID
    and emulated module)
  - [Client] Start the server
  - [Client] Read the local overrides
  - [Client] Wait for the socket to be up and connect to it
  - [Client] Query the server asking for all the objects that will be proxied.
    Only the server knows because the file defining the whitelisted objects
    includes the library that the client cannot load.
  - [Server] Read the server overrides as well as the whitelisted information.
    This process is somewhat involved due to the way we handle exceptions
    (allowing for hierarchy information in exceptions).
  - [Server] Setting up handlers
  - [Server] Opening the UNIX socket and waiting for a connection
  - [Server] Once a connection is established, waiting for request. The server
    is single threaded by design (it is an extension of the client which is
    single threaded).

  At this point, the connection is established but nothing has happened yet.
  Modules have not yet been overloaded. This is described in the next section.

### Module injection

  The file ```client_modules.py``` contains all the magic required to overload
  and inject modules. It is designed in such a way that the Client (and
  therefore Server) are only created when the user does ```import
  data_accessor``` (in our example).

  Metaflow will call ```create_modules``` when launching Conda. This doesn't
  actually inject any modules but registers a module loader with Python telling
  it: "if you need to load a module that starts with this name, call me". In
  other words, if the user types ```import data_accessor``` and Metaflow
  registered a handler on the name ```data_accessor```, the code in
  ```load_module``` (in ```client_modules.py```) will get called.

At that point, a Client/Server pair will be spun up and the Client will be used
to determine everything that needs to be overloaded. A ```_WrappedModule``` will
be created which pretends it is a module (it's really just a class) and which
will contain everything that is whitelisted for this module. In particular, it
contains code to create stub classes on the fly when requested (when possible,
everything is done lazily to avoid paying the cost of something that is not
used).

## Defining an emulated module

To define an emulated module, you need to create a subdirectory in
```plugins/env_escape/configurations``` called ```emulate_<name>``` where
```<name>``` is the name of the library you want to emulate. It can be a "list"
where ```__``` is the list separator; this allows multiple libraries to be
emulated within a single server environment.

Inside this directory, apart from the usual ```__init__.py```, you need to
create two files:
- ```server_mappings.py``` which must contain the following five fields:
  - ```EXPORTED_CLASSES```: This is a dictionary of dictionary describing the
    whitelisted classes. The outermost key is either a string or a tuple of
    strings and corresponds to the "module" name (it doesn't really have to be
    the module but the prefix of the full name of the whitelisted class). The
    inner key is a string and corresponds to the suffix of the whitelisted
    class. Finally, the value is the class to which the class maps internally. If
    the outermost key is a tuple, all strings in that tuple will be considered
    aliases of one another.
  - ```EXPORTED_FUNCTIONS```: This is the same structure as
    ```EXPORTED_CLASSES``` but contains module level functions.
  - ```EXPORTED_VALUES```: Similar for module level attributes
  - ```PROXIED_CLASSES```: A tuple of other objects that the server can return
  - ```EXPORTED_EXCEPTIONS```: Same structure as ```EXPORTED_CLASSES``` and
    contains the exceptions that will be exported explicitly (and recreated as
    such) on the other side. Note that methods on exceptions are not recreated
    (they are not like classes) to avoid going back to the server after an
    exception occurs. The hierarchy of the exceptions specified here will be
    maintained and, as such, you must specify all exceptions up to a basic
    Exception type.
- ```overrides.py```: This file contains ```local_override```,
  ```local_getattr_override```, ```local_setattr_override``` and their remote
  counterparts, ```local_exception``` and ```remote_exception_serialize``` (all
  defined in ```override_decorators.py```).

  ```local_override``` and ```remote_override``` allow you to define the method
  overrides. They are function-level decorators and take as argument a
  dictionary where the key is the class name and the value is the method name
  (both strings). Note that if you override a static or a class method, the
  arguments passed to the function are different. For local overrides:
  - for regular methods, the arguments are ```(stub, func, *args, **kwargs)```;
  - for static methods, the arguments are ```(func, *args, **kwargs)```;
  - for class methods, the arguments are ```(cls, func, *args, **kwargs)```
    where ```cls``` is the class of the stub (not very useful).

  This is similar for remote overrides (except objects are passed instead of
  stubs).

  ```local_getattr_override``` and ```local_setattr_override``` allow you to
  define how attributes are accessed. Note that this is not restricted to
  attributes accessed using the ```getattr``` and ```setattr``` functions but
  any attribute. Both of these functions take as arguments ```stub```,
  ```name``` and ```func``` which is the function to call in order to call the remote
  ```getattr``` or ```setattr```. The ```setattr``` version takes an additional
  ```value``` argument. The remote versions simply take the target object and
  the name of the attribute (and ```value``` if it is a ```setattr``` override)
  -- in other words, they look exactly like ```getattr``` and ```setattr```.
  Note that you have to call ```getattr``` and ```setattr``` yourself on the
  object.

  ```local_exception``` and ```remote_exception_serialize``` allow you to define
  a class to be used for specific exceptions as well as pass user data (via a
  side-band) to the exception from the server to the client. The
  ```local_exception``` decorator takes the full name of the exception to
  override as a parameter. This is a class-level decorator and all attributes
  and methods defined in the class will be added to those brought back from the
  server for this particular exception type. If you define something that
  already exists in the exception, the server value will be stored in
  ```_original_<name>```. As an example, if you define ```__str__``` in your
  class, you can access ```self._original___str__``` which will be the string
  representation fetched from the server. You can also define a special method
  called ```_deserialize_user``` which should take a JSON decoded object and is
  the mirror method of the ```remote_exception_serialize``` decorator.

  Finally, the ```remote_exception_serialize``` decorator takes a single
  argument, the name of the exception. It applies to a function that should take
  a single argument, the exception object itself and return a JSON-encodable
  object that will be passed to ```_deserialize_user```. You can use this to
  pass any additional information to the client about the exception.

Metaflow will load all modules in the ```configurations``` directory that start
with ```emulate_```.
