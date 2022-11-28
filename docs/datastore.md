# Datastore design
## Motivation

The datastore is a crucial part of the Metaflow architecture and deals with
storing and retrieving data, be they artifacts (data produced or consumed within
user steps), logs, metadata information used by Metaflow itself to track execution
or other data like code packages.

One of the key benefits of Metaflow is the ease with which users can access the
data; it is made available to steps of a flow that need it and users can access
it using the Metaflow client API.

This documentation provides a brief overview of Metaflow's datastore implementation
and points out ways in which it can be extended to support, for example, other
storage systems (like GCS instead of S3).

## High-level design

### Design principles
A few principles were followed in designing this datastore. They are listed here
for reference and to help explain some of the choices made.

#### Backward compatibility
The new datastore should be able to read and interact with data stored using
an older implementation of the datastore. While we do not guarantee forward
compatibility, currently, older datastores should be able to read most of the data
stored using the newer datastore.

#### Batch operations
Where possible, APIs are batch friendly and should be used that way. In other
words, it is typically more efficient to call an API once, passing it all the
items to operate on (for example, all the keys to fetch) than to call the same
API multiple times with a single key at a time. All APIs are designed with
batch processing in mind where it makes sense.

#### Separation of responsibilities
Each class implements few functionalities, and we attempted to maximize reuse.
The idea is that this will also help in developing newer implementations going
forward and being able to surgically change a few things while keeping most of
the code the same.

### Storage structure
Before going into the design of the datastore itself, it is worth considering
**where** Metaflow stores its information. Note that, in this section, the term
`directory` can also refer to a `prefix` in S3 for example.

Metaflow considers a datastore to have a `datastore_root` which is the base
directory of the datastore. Within that directory, Metaflow will create multiple
subdirectories, one per flow (identified by the name of the flow). Within each
of those directories, Metaflow will create one directory per run as well as
a `data` directory which will contain all the artifacts ever produced by that
flow.

The datastore has several components (starting at the lowest-level):
- a `DataStoreStorage` which abstracts away a storage system (like S3 or
  the local filesystem). This provides very simple methods to read and write
  bytes, obtain metadata about a file, list a directory as well as minor path
  manipulation routines. Metaflow provides sample S3 and local filesystem
  implementations. When implementing a new backend, you should only need to
  implement the methods defined in `DataStoreStorage` to integrate with the
  rest of the Metaflow datastore implementation.
- a `ContentAddressedStore` which implements a thin layer on top of a
  `DataStoreStorage` to allow the storing of byte blobs in a content-addressable
  manner. In other words, for each `ContentAddressedStore`, identical objects are
  stored once and only once, thereby providing some measure of de-duplication.
  This class includes the determination of what content is the same or not as well
  as any additional encoding/compressing prior to storing the blob in the
  `DataStoreStorage`. You can extend this class by providing alternate methods of
  packing and unpacking the blob into bytes to be saved.
- a `TaskDataStore` is the main interface through which the rest of Metaflow
  interfaces with the datastore. It includes functions around artifacts (
  `persisting` (saving) artifacts, loading (getting)), logs and metadata.
- a `FlowDataStore` ties everything together. A `FlowDataStore` will include
  a `ContentAddressedStore` and all the `TaskDataStore`s for all the tasks that
  are part of the flow. The `FlowDataStore` includes functions to find the
  `TaskDataStore` for a given task as well as to save and load data directly (
  this is used primarily for data that is not tied to a single task, for example
  code packages which are more tied to runs).

From the above description, you can see that there is one `ContentAddressedStore`
per flow so artifacts are de-duplicated *per flow* but not across all flows.

## Implementation details

In this section, we will describe each individual class mentioned above in more
detail

### `DataStoreStorage` class

This class implements low-level operations directly interacting with the
file-system (or other storage system such as S3). It exposes a file and
directory like abstraction (with functions such as `path_join`, `path_split`,
`basename`, `dirname` and `is_file`).

Files manipulated at this level are byte objects; the two main functions `save_bytes`
and `load_bytes` operate at the byte level. Additional metadata to save alongside
the file can also be provided as a dictionary. The backend does not parse or
interpret this metadata in any way and simply stores and retrieves it.

The `load_bytes` has a particularity in the sense that it returns an object
`CloseAfterUse` which must be used in a `with` statement. Any bytes loaded
will not be accessible after the `with` statement terminates and so must be
used or copied elsewhere prior to termination of the `with` scope.

### `ContentAddressedStore` class

The content addressed store also handles content as bytes but performs two
additional operations:
 - de-duplicates data based on the content of the data (in other words, two
   identical blobs of data will only be stored once
 - transforms the data prior to storing; we currently only compress the data but
   other operations are possible.
   
Data is always de-duplicated, but you can choose to skip the transformation step
by telling the content address store that the data should be stored `raw` (ie:
with no transformation). Note that the de-duplication logic happens *prior* to
any transformation (so the transformation itself will not impact the de-duplication
logic).

Content stored by the content addressed store is addressable using a `key` which is
returned when `save_blobs` is called. `raw` objects can also directly be accessed
using a `uri` (also returned by `save_blobs`); the `uri` will point to the location
of the `raw` bytes in the underlying `DataStoreStorage` (so, for example, a local
filesystem path or a S3 path). Objects that are not `raw` do not return a `uri`
as they should only be accessed through the content addressed store.

The symmetrical function to `save_blobs` is `load_blobs` which takes a list of
keys (returned by `save_blobs`) and loads all the objects requested. Note that
at this level of abstraction, there is no `metadata` for the blobs; other
mechanisms exist to store, for example, task metadata or information about
artifacts.

#### Implementation detail

The content addressed store contains several (well currently only a pair) of
functions named `_pack_vX` and `_unpack_vX`. They effectively correspond to
the transformations (both transformation to store and reverse transformation
to load) the data undergoes prior to being stored. The `X` corresponds to the
version of the transformation allowing new transformations to be added easily.
A backward compatible `_unpack_backward_compatible` method also allows this
datastore to read any data that was stored with a previous version of the
datastore. Note that going forward, if a new datastore implements `_pack_v2` and
`_unpack_v2`, this datastore would not be able to unpack things packed with
`_pack_v2` but would throw a clear error as to what is happening.

### `TaskDataStore` class

This is the meatiest class and contains most of the functionality that an executing
task will use. The `TaskDataStore` is also used when accessing information and
artifacts through the Metaflow Client.

#### Overview

At a high level, the `TaskDataStore` is responsible for:
 - storing artifacts (functions like `save_artifacts`, `persist` help with this)
 - storing other metadata about the task execution; this can include logs,
   general information about the task, user-level metadata and any other information
   the user wishes to persist about the task. Functions for this include
   `save_logs` and `save_metadata`. Internally, functions like `done` will
   also store information about the task.

Artifacts are stored using the `ContentAddressedStore` that is common to all
tasks in a flow; all other data and metadata is stored using the `DataStoreStorage`
directly at a location indicated by the `pathspec` of the task.

#### Saving artifacts

To save artifacts, the `TaskDataStore` will first pickle the artifacts, thereby
transforming a Python object into bytes. Those bytes will then be passed down
to the `ContentAddressedStore`. In other words, in terms of data transformation:
 - Initially you have a pickle-able Python object
 - `TaskDataStore` pickles it and transforms it to `bytes`
 - Those `bytes` are then de-duped by the `ContentAddressedStore`
 - The `ContentAddressedStore` will also gzip the `bytes` and store them
   in the storage backend.

Crucially, the `TaskDataStore` takes (and returns when loading artifacts)
Python objects whereas the `ContentAddressedStore` only operates with bytes.

#### Saving metadata and logs

Metadata and logs are stored directly as files using the `DataStoreStorage` to create
and write to a file. The name of the file is something that `TaskDataStore`
determines internally.

### `FlowDataStore` class

The `FlowDataStore` class doesn't do much except give access to `TaskDataStore`
(in effect, it creates the `TaskDataStore` objects to use) and also allows
files to be stored in the `ContentAddressedStore` directly. This is used to
store, for example, code packages. File stored using the `save_data` method
are stored in `raw` format (as in, they are not further compressed). They will,
however, still be de-duped.

### Caching

The datastore allows the inclusion of caching at the `ContentAddressedStore` level:
 - for blobs (basically the objects returned by `load_blobs` in the
   `ContentAddressedStore`). Objects in this cache have gone through: reading
   from the backend storage system and the data transformations in
   `ContentAddressedStore`.

The datastore does not determine how and where to cache the data and simply
calls the functions `load_key` and `store_key` on a cache configured by the user
using `set_blob_cache`.
`load_key` is expected to return the object in the cache (if present) or None otherwise.
`store_key` takes a key (the one passed to `load`) and the object to store. The
outside cache is free to implement its own policies and/or own behavior for the
`load_key` and `store_key` functions.

As an example, the `FileCache` uses the `blob_cache` construct to write to
a file anything passed to `store_key` and returns it by reading from the file
when `load_key` is called. The persistence of the file is controlled by the
`FileCache` so an artifact `store_key`ed may vanish from the cache and would
be re-downloaded by the datastore when needed (and then added to the cache
again).
