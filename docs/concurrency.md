# Concurrency in the Metaflow Codebase

Here's a definition of concurrency and its sibling
concept parallelism:
*Concurrency is the composition of independently executing processes,
while parallelism is the simultaneous execution of (possibly related)
computations* from
[a talk by Rob Pike, Concurrency is not Parallelism](https://blog.golang.org/concurrency-is-not-parallelism):

**Parallelism** is a relatively straightforward and quantifiable
concept. However, it is not always easy to decide what constructs of
**concurrency**, which can lead to parallelism, are most appropriate
in each context. The choice is not easy since besides parallelism
and performance, we also want to optimize our code for robustness,
observability, maintainability, and readability.

This document describes the constructs of concurrency that are used
in the Metaflow codebase. If you need to leverage concurrency in the
internals of Metaflow, this document should help you to choose the right
tool for the job. However, we do **not encourage** you to introduce
concurrency unless it is clearly necessary. It is much easier to write
simple, readable, and robust non-concurrent code compared to anything
concurrent.

[Make it work, make it right, make it fast](http://wiki.c2.com/?MakeItWorkMakeItRightMakeItFast).
Concurrency is practically never needed during the first two phases.

## Vocabulary

We divide the concurrency constructs into two categories: Primary and
Secondary. Whenever possible, you should prefer the constructs in
the first category. The patterns are well established and have
been used successfully in the core Metaflow modules, `runtime.py`
and `task.py`. The constructs in the second category can be used in
subprocesses, outside the core code paths in `runtime.py` and `task.py`.
The reasons for this are elaborated below.

In this document, we call an atomic unit of concurrent execution
**a task**. A task is an operation that we want to execute concurrently
with other operations. In this sense, tasks are equivalent to
[`asyncio.Task`s in Python](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task),
[Goroutines in Go](https://tour.golang.org/concurrency/1), and
[Processes in Erlang](https://erlangbyexample.org/processes).
Coincidentally, Metaflow `Task`s run by `task.py` are also tasks in this
sense but we have also many other internal tasks in Metaflow besides
the `Task` that executes the user code.

For a quick overview, see the [summary](#summary) below.

## Primary Constructs for Concurrency

These patterns power the core Metaflow functionality in `runtime.py`
and `task.py`. They are also fully observable: You can easily see what
concurrent tasks are running, and you can re-launch individual tasks for
testing and reproduction of issues.

### 1. Subprocesses for subcommands

Metaflow uses its own CLI to execute tasks as subprocesses. There are
two main benefits of this approach:

 1. Subprocesses are fully isolated from the parent process, so they can
    execute arbitrary user code. Besides intentionally malicious code and
    resource exhaustion, there is no way for the child process to crash
    the parent, which is critically important for Metaflow.

 2. Subprocesses can be launched by different parents easily, thanks to the
    standard CLI "API". We leverage this feature to launch subprocesses
    on Titus and via Meson.

#### Example Uses

The subcommand `step` is used to execute individual Metaflow tasks. This
subcommand is also used to clone many datastores concurrently during
`resume`. These subprocesses are managed by `runtime.Worker`.

#### How to Observe

Set the environment variable `METAFLOW_DEBUG_SUBCOMMAND=1` to see the
exact command line that is used to launch a subcommand task. You can
re-execute the task simply by re-executing the command line manually.
However, be careful when re-executing commands from real runs, as you
will rewrite data in the datastore. To be safe, preferably rerun only 
commands executed with `--datastore=local` and `--metadata=local`.

You can observe running subprocesses with `ps` and attach to them using
`gdb` as usual. Or you can kill them e.g. with `kill -9`.

#### Intended Use Cases

Subcommands work best if there is very limited communication between the
parent and the child process. No message passing between the processes
is supported currently.

### 2. Sidecars

Sidecars were introduced to address the need to execute internal tasks
in parallel with scheduling in `runtime.py` or during the execution of
user code in `task.py`. Especially in the latter case the user code may
block the Python interpreter for an arbitrary amount of time, so there
isn't a safe way to execute internal tasks in the same interpreter. As a
solution, we use child processes to host these tasks, aka sidecars.

The lifetime of a sidecar is bound to the lifetime of its parent
process. In contrast to subcommands, there is a one-way, lossy,
communication channel from the parent to the sidecar. Sidecar
implementations are expected to consume messages from the parent without
delay, to avoid the parent from blocking.

The sidecar subprocess may die for various reasons, in which case
messages sent to it by the parent may be lost. To keep communication
essentially non-blocking and fast, there is no blocking acknowledgement of
successful message processing by the sidecar. Hence the communication is
lossy. In this sense, communication with a sidecar is more akin to UDP
than TCP.

#### Example Uses

We send heart beats to metadata service in a sidecar, `heartbeat.py` to
detect whether the task is alive. Since heart beats are purely informational,
we didn't want to increase the latency of the main process due to these 
service calls, nor we wanted to fail the whole parent process in case of a 
request failing. A sidecar that handles communication with the metadata 
service was a perfect solution.

#### How to Observe

Set the environment variable `METAFLOW_DEBUG_SIDECAR=1` to see the
commands used to launch sidecars. You can send messages to the sidecar
via `stdin`. However, be mindful about not polluting production systems
with test data when testing sidecars.

You can observe running sidecars with `ps` and attach to them using
`gdb` as usual. Or you can kill them e.g. with `kill -9`.

#### Intended Use Cases

Use a sidecar if you need a task that runs during scheduling or
execution of user code. A sidecar task can not perform any critical
operations that must succeed in order for a task or a run to be
considered valid. This makes sidecars suitable only for opportunistic,
best-effort tasks.

### 3. Data Parallelism

Many use cases of concurrency are related to IO: we want to load or
store N objects in parallel. Instead of hiding data parallelism in
generic constructs of concurrency, e.g. a thread pool, we can leverage
specific constructs optimized for this use case.

In the case of Metaflow, data parallelism is most often related to
Amazon S3 which is our main `datastore`. Luckily, Metaflow comes with
[a built-in S3 client](https://docs.metaflow.org/metaflow/data#data-in-s-3-metaflow-s3)
that provides methods like `get_many` that handle concurrency automatically.

#### Example Uses

The `MetaflowDatastoreSet` class represents a set of datastores which
can be loaded concurrently. Using this class instead of loading each
`Datastore` sequentially has yielded a significant performance boost in
`resume` and normal task execution.

#### How to Observe

Set the environment variable `METAFLOW_DEBUG_S3CLIENT=1` to see the
commands used to interact with S3 through the built-in client. Note
that this setting will also persist temporary control files passed to
the client, to make it easier to reproduce and observe the client's
behavior. However, you will need to clean up the temporary files,
prefixed with `metaflow.s3`, manually.

The client uses a CLI of `s3op.py` internally, which you can test with

```
python -m metaflow.datatools.s3op
```

You can observe running S3 operations with `ps` and attach to them using
`gdb` as usual. Or you can kill them e.g. with `kill -9`.

#### Intended Use Cases

Use data parallelism provided by `S3.get_many` / `S3.put_many` when you
need to perform multiple S3 operations. S3 really shines at providing
maximum performance for a high number of parallel operations.

## Secondary Constructs for Concurrency

The following constructs can be used in sidecars and other subprocesses
of Metaflow. They are not well-suited for being used in `runtime.py` and
`task.py` directly, as explained below.

### 4. Threads

The internal state of the Python interpreter
is guarded by [the Global Interpreter Lock, or
GIL](https://wiki.python.org/moin/GlobalInterpreterLock). The main
effect of the GIL is that in most cases two distinct threads executing
Python can't run in parallel, which limits the usefulness of threads in
Python. Even if this wasn't the case, [threads are hard to use
correctly](https://www.google.com/search?q=threads+are+evil).

However, as a construct of concurrency, if not parallelism, threads
have some uses. The main upside of threads is that communication between
tasks is very easy and practically zero-cost.

#### Example Uses

Many sidecars, e.g. `heartbeat.py`, use a separate worker thread to make 
sure that the main process consuming messages from the parent will not 
block for an extended amount of time.

### 5. Multiprocessing

The `multiprocessing` module in Python is a (thick) layer of abstraction
over subprocesses. The main upside of `multiprocessing` is that it is
not limited by the Global Interpreter Lock, so it can leverage
multi-process/multi-core parallelism.

The main downside of `multiprocessing` is that it tries to provide a
very high-level abstraction over processes, which is surprisingly hard
to do well. For this reason, historically, the implementation has not
been bug-free. Even though the implementation has improved over time, it
has still rough edges: e.g. messages need to be picklable, their sizes
are limited, called functions need to be at the top level of the module
etc. Also, debugging `multiprocessing` code can be hard compared to
plain subprocesses.

Use `multiprocessing` in your subprocesses if you absolutely need one of
the advanced constructs, such as multi-consumer `Queue`. For simple use
cases, simple subprocesses are almost always a better choice.

#### Example Uses

The Metaflow S3 client, `s3op.py`, uses `multiprocessing` internally
to manage its internal worker processes.

### 6. `parallel_map`

A close cousin of `multiprocessing` is [`metaflow.parallel_map`](https://docs.metaflow.org/metaflow/scaling#parallelization-over-multiple-cores).
In contrast to `multiprocessing`, child processes are simply `fork`'ed
instead of executed as subprocesses. The main upside of this approach
is that passing data, including the function defining the task, has no
limitations and only a negligible cost, since no serialization is
involved. However, passing data back to the parent involves pickling,
similar to `multiprocessing`.

However, [the semantics of `fork` can be finicky](https://codewithoutrules.com/2018/09/04/python-multiprocessing/).
For this reason, we want to avoid using `parallel_map` in the core
Metaflow.

### 7. Async

Python 3 introduced [asynchronous programming as the first-class
citizen in Python](https://docs.python.org/3/library/asyncio.html). At its
core, `asyncio` is a scheduler for cooperative multitasking. The main
upside of `asyncio` is that it makes concurrency very explicit: the code
can include explicit `Task` objects that yield (`await`) control to other
tasks when they see fit. This style of concurrency is particularly well
suited for IO-bound network programming, e.g. web servers, which need to
execute many request handler tasks concurrently, more so than in parallel.

The downsides of `asyncio` are many:

 - `asyncio` is not available in Python 2 and its standard library
   implementation has been quickly evolving at least until Python 3.6.
   This makes it practically unusable in Metaflow, which needs to support
   Python 2 and earlier versions of Python 3.

 - `asyncio` requires a lot of attention from the programmer. It is very
   easy to introduce issues that tank the performance (e.g. a single blocking
   function call), produce extremely hard to debug bugs (e.g. forget to catch
   an exception), and/or random deadlocks (e.g. wait on a shared resource).

 - By default, `asyncio` is useless for CPU-bound tasks. It needs to rely
   on a thread- or a process-pool to achieve CPU-parallelism. One could use
   a thread or a process-pool directly and avoid many pitfalls of `asyncio`.

`asyncio` has its uses in servers outside Metaflow. Currently it is
not suitable to be used in the core Metaflow.

## Summary

The table below summarizes the discussion. We focus on comparing four key
features of the concurrency constructs:

 - **Arbitrary code** - does the construct provide enough isolation that
   it can be used to execute arbitrary, user-defined, Python-code safely.

 - **Return data** - does the construct allow returning data to the
   caller after the task has finished.

 - **Message passing** - does the construct support communication between
   tasks during the execution of tasks.

 - **Observable** - is it possible to observe what tasks are running and
   re-execute individual tasks easily, e.g. to reproduce issues.

```
Construct        Arbitrary code   Return data   Message passing  Observable

PRIMARY
Subprocesses          yes          partial(1)        no             yes
Sidecars            partial(2)       no            partial(3)     partial(4)
Data Parallelism      no             yes             no             yes

SECONDARY
Threads               no             yes             yes            no
Multiprocessing       yes          partial(5)      partial(5)       no
parallel_map        partial(6)     partial(7)        no             no
Async                 no             yes             yes            no
```

1. We record only the exit code of a subprocess. Data can not be returned
   directly.
2. Sidecars need to be well-behaving: they need to consume messages from
   the parent without delay.
3. Sidecars support only lossy, one-way message passing from the parent
   to the sidecar.
4. In contrast to subprocesses and data parallelism, the command line does
   not provide sufficient information to reconstruct the exact state of a
   sidecar. This would require replaying of all messages sent to the sidecar.
5. Values communicated via `multiprocessing` need to be picklable. There
   are other limitations and issues related to the `Queue` object, which is
   used to facilitate communication.
6. Due to finicky semantics of `fork`, the child process is only
   partially isolated from the parent which makes `parallel_map` a bad
   candidate for execution of arbitrary code.
7. Values returned by `parallel_map` need to be picklable.

