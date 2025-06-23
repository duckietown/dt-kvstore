# KVStore Node

This node implements a **file-based key–value store** backed by DTPS (Duckietown Postal Service). It watches a directory on disk (`/data/config`), automatically creates adapters for any matching files (generic/plain, YAML, or JSON), and exposes corresponding DTPS topics so that other components can read or update those files at runtime. In addition, it provides two RPC endpoints:

* **`define`**: create or update a file at a given key with a given value (YAML format by default).
* **`drop`**: remove a file (and its associated DTPS topic).

Under the hood, each file is wrapped in a `FileAdapter`, which reads its contents on startup, strips any leading/trailing whitespace, and publishes its current value onto DTPS. Whenever a topic update arrives, the adapter writes the new content back to disk. This makes it easy to treat on-disk configuration files as live key–value objects.

---

### Table of Contents

- [KVStore Node](#kvstore-node)
    - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
    - [Summary](#summary)
  - [Directory Structure \& Patterns](#directory-structure--patterns)
    - [Predefined Patterns](#predefined-patterns)
  - [Supported File Types](#supported-file-types)
  - [FileAdapter Classes](#fileadapter-classes)
    - [GenericFileAdapter (Base)](#genericfileadapter-base)
    - [PlainFileAdapter](#plainfileadapter)
    - [YAMLFileAdapter](#yamlfileadapter)
    - [JSONFileAdapter](#jsonfileadapter)
  - [How It Works](#how-it-works)
    - [Startup \& Scanning `/data/config`](#startup--scanning-dataconfig)
    - [Adapter Instantiation \& Lifecycle](#adapter-instantiation--lifecycle)
    - [“define” RPC (Create/Update)](#define-rpc-createupdate)
    - [“drop” RPC (Delete)](#drop-rpc-delete)
    - [Handling New/Removed Topics](#handling-newremoved-topics)
    - [UDP Responder](#udp-responder)
  - [Content of the Files](#content-of-the-files)
  - [Usage Examples](#usage-examples)
    - [1. Reading an Existing YAML Configuration](#1-reading-an-existing-yaml-configuration)
    - [2. Updating a Plain‐Text File (`robot_type`)](#2-updating-a-plaintext-file-robot_type)
    - [3. Creating a New Configuration via “define”](#3-creating-a-new-configuration-via-define)
    - [4. Deleting a Configuration via “drop”](#4-deleting-a-configuration-via-drop)
  - [Environment \& Logging](#environment--logging)

---

## Overview

The **KVStore node** is designed to turn plain files on disk into live, network‐accessible key–value topics. In practice:

* Any file under `/data/config` that matches one of the configured regex patterns will be “adapted” into a DTPS topic.
* Depending on its filename/location, a file is treated as plain text, YAML, or JSON.
* On startup, the node scans `/data/config`, instantiates the appropriate adapter for each matched file, and immediately publishes that file’s contents to its corresponding DTPS topic.
* When a DTPS client publishes a new payload to that topic, the adapter writes the updated content back to the same file on disk.
* Two RPC topics—`define` and `drop`—allow other components to create new files (with initial content) or delete existing ones at runtime, without restarting the node.
* A small UDP responder is started so that external components can discover or interact with this node via UDP (e.g. service discovery or heartbeats).


### Summary

* **What this node does**:

  * Scans `/data/config`, instantiates adapters for any matching files (plain, YAML, JSON).
  * Publishes each file’s contents on a corresponding DTPS topic.
  * Listens for updates on those topics and writes changes back to disk.
  * Provides two RPCs, `define` (create/update) and `drop` (delete), for dynamic file management at runtime.
  * Runs a UDP responder for service discovery or heartbeats.

* **Supported file types**:

  1. **Plain (generic) text** – stored/read as raw UTF-8.
  2. **YAML** – parsed via `yaml.safe_load` and re‐written with `yaml.dump(sort_keys=True)`.
  3. **JSON** – (if enabled by adding a regex) parsed via `json.loads` and re‐written with `json.dumps(indent=4, sort_keys=True)`.

* **Expected structure**:

  * Files under `/data/config/node/<key>/<ROBOT_NAME>.yaml` → YAML with node‐specific config → topic `data/node/{key}/config`.
  * Files under `/data/config/permissions/<key>` → plain text → `data/permission/{key}`.
  * Files under `/data/config/calibrations/<key>/<ROBOT_NAME>.yaml` → YAML calibration → `data/calibration/{key}/current` (with defaults copied if missing).
  * Files under `/data/config/calibrations/<key>/default.yaml` → YAML defaults → `data/calibration/{key}/default`.
  * Plain files called `robot_<something>` → `data/robot/{something}`.
  * All other `*.yaml` under `/data/config` → `data/{key}` (generic YAML).

With this setup, any on-disk configuration file instantly becomes a live, networked DTPS topic, easily readable and updatable by other Duckietown components.


## Directory Structure & Patterns

By default, **all files** live under:

```
/data/config
```

(This path is hardcoded as `ADAPTED_FILES_DIR = "/data/config"` in the source.)

The node uses a dictionary of **regex → FileAdapterTemplate** entries. Each regex pattern is matched against the full (absolute) filepath. If it matches, the code uses the regex’s named groups to compute:

1. **`object_path`**: the DTPS topic namespace where the file’s current contents will be published.
2. **`kind`**: which `FileAdapter` subclass to use (Plain, YAML, or JSON).
3. **`droppable`**: whether this file can be removed at runtime.
4. **`default`** (optional): a mapping of “default file paths” → initial contents (for calibration files). If a default file is not found on disk, the adapter will create it from this initial content.

### Predefined Patterns

Below are the patterns (in insertion order). Note that the code always tries them in sequence and uses the **first match** it finds for each file. The final entry is a “catch-all” that matches any other `*.yaml` file.

1. `/data/config/node/(?P<key>.*)/<ROBOT_NAME>.yaml`

   * **Adapter**: `YAMLFileAdapter`
   * **Topic**: `data/node/{key}/config`
   * **Droppable**: `True`
   * **default**: `NOTSET`
   * *Example filepath:*

     ```
     /data/config/node/web_server/duckiebot1.yaml
     → publishes on DTPS topic “data/node/web_server/config”
     ```

2. `/data/config/permissions/(?P<key>.*)`

   * **Adapter**: `PlainFileAdapter`
   * **Topic**: `data/permission/{key}`
   * **Droppable**: `False`
   * **default**: `NOTSET`
   * *Example filepath:*

     ```
     /data/config/permissions/camera
     → publishes raw bytes (as UTF-8 string) on “data/permission/camera”
     ```

3. `/data/config/calibrations/(?P<key>.*)/<ROBOT_NAME>.yaml`

   * **Adapter**: `YAMLFileAdapter`
   * **Topic**: `data/calibration/{key}/current`
   * **Droppable**: `True`
   * **default**:

     * `/data/config/calibrations/camera_intrinsic/<ROBOT_NAME>.yaml`
       → loaded from `/data/config/calibrations/camera_intrinsic/default.yaml` on disk.
     * `/data/config/calibrations/camera_extrinsic/<ROBOT_NAME>.yaml`
       → loaded from `/data/config/calibrations/camera_extrinsic/default.yaml`.
     * `/data/config/calibrations/kinematics/<ROBOT_NAME>.yaml`
       → loaded from `/data/config/calibrations/kinematics/default.yaml`.
   * *Example filepath:*

     ```
     /data/config/calibrations/camera_intrinsic/duckiebot1.yaml
     → if missing, the node copies in the “default.yaml” for camera_intrinsic, then
       publishes that as “data/calibration/camera_intrinsic/current”
     ```

4. `/data/config/calibrations/(?P<key>.*)/default.yaml`

   * **Adapter**: `YAMLFileAdapter`
   * **Topic**: `data/calibration/{key}/default`
   * **Droppable**: `False`
   * **default**: `NOTSET`
   * *Example filepath:*

     ```
     /data/config/calibrations/camera_intrinsic/default.yaml
     → publishes the default‐calibration YAML on “data/calibration/camera_intrinsic/default”
     ```

5. `/data/config/robot_(?P<key>.*)`

   * **Adapter**: `PlainFileAdapter`
   * **Topic**: `data/robot/{key}`
   * **Droppable**: `False`
   * **default**: `NOTSET`
   * *Example filepath:*

     ```
     /data/config/robot_type
     → publishes “duckiebot” (or whatever text is inside that file) on “data/robot/type”
     ```

6. `/data/config/(?P<key>.*).yaml`  (“catch-all” for any other YAML)

   * **Adapter**: `YAMLFileAdapter`
   * **Topic**: `data/{key}`
   * **Droppable**: `True`
   * **default**: `NOTSET`
   * *Example filepath:*

     ```
     /data/config/network.yaml
     → publishes parsed YAML on “data/network”
     ```

---

## Supported File Types

The node supports **three** main categories of files:

1. **Generic / Plain‐Text** (any file that does **not** end with “`.yaml`” and matches a PlainFileAdapter pattern):

   * Contents are treated as a raw UTF-8 string (no further parsing).
   * Example: `/data/config/robot_type` might contain:

     ```
     duckiebot
     ```

     This will be published under “`data/robot/type`” as the string `"duckiebot"`.

2. **YAML Files** (any file whose path matches one of the YAML patterns above):

   * Contents are parsed via `yaml.safe_load(...)` into a Python object (dict, list, etc.).
   * Any update published on the corresponding DTPS topic must also be a valid YAML object (or primitive).
   * When written back to disk, the adapter invokes `yaml.dump(...)` (with `sort_keys=True`), so the file is always normalized (sorted keys, minimal quoting).
   * Example:

     ```yaml
     # /data/config/calibrations/camera_intrinsic/duckiebot1.yaml
     camera_matrix:
       fx: 628.2
       fy: 629.4
       cx: 322.3
       cy: 235.1
     distortion_coeffs:
       - -0.2
       - 0.1
       - 0.0
       - 0.0
     ```

     —published on “`data/calibration/camera_intrinsic/current`”.

3. **JSON Files** (only if you manually extend the patterns to include `.json`; by default, the node is configured for YAML, but the `JSONFileAdapter` is included for completeness):

   * Contents are parsed via `json.loads(...)` into a Python object (dict, list, etc.).
   * On disk, any update is re‐serialized with `json.dumps(..., sort_keys=True, indent=4)` to maintain a human-readable JSON format.
   * Example (if a topic is mapped to a `.json` file):

     ```json
     {
         "threshold": 0.5,
         "modes": ["auto", "manual"],
         "enabled": true
     }
     ```

     —published on the corresponding topic.

> **Note:** By default, there is no regex entry to match `.json` under `/data/config`. If you want to support JSON files, you can simply add another entry to `ADAPTED_FILES` in the code with a pattern like
>
> ```
> f"{ADAPTED_FILES_DIR}/(?P<key>.*).json": FileAdapterTemplate(
>     object_path="data/{key}",
>     properties=FullRW,
>     kind=JSONFileAdapter,
>     droppable=True,
> )
> ```

---

## FileAdapter Classes

All adapters inherit from `GenericFileAdapter`, which handles:

* Checking if the file exists on disk.
* Optionally creating it (with default or initial content).
* Reading raw bytes, stripping whitespace, converting to a “native” Python object, and re‐serializing back to bytes.
* Publishing initial contents to DTPS and subscribing for updates.
* Writing updates back to disk.

Below are the three concrete subclasses:

### GenericFileAdapter (Base)

* **Fields / Constructor Arguments**:

  * `file_path: str`
  * `object_path: str` (DTPS topic, e.g. `data/node/web_server/config`)
  * `properties: Optional[TopicProperties]` (controls read/write permissions in DTPS)
  * `persist: bool` (if `False`, skip reads/writes on disk)
  * `droppable: bool` (if `False`, calling `drop()` will do nothing)
  * `create: bool = False` (if file does not exist on disk, raise unless `create=True`)
  * `default: Optional[object]` (initial content if the file is missing; used only if `create=True`)
  * `initial: Optional[object]` (alternative “initial” content for newly created files)

* **Lifecycle**:

  1. **`__post_init__`**

     * If the file does not exist:

       * If `create=False`, raise `FileNotFoundError`.
       * Otherwise, make parent directories, and if `initial` or `default` is set, call `raw_from_native_object(...)` → store in `_content` → `write_to_disk()`.
     * If the file *does* exist:

       * Call `read_from_disk()` → `_content` is raw bytes with trailing newlines/whitespace if any.
       * Immediately `.strip(" \n\r\t")` to remove leading/trailing spaces or newlines.
       * Re‐serialize by calling `self._content = raw_from_native_object(to_native_object())` to normalize formatting.
  2. **`read_from_disk()`**

     * If `persist=True`, open the file in **binary** mode and load all bytes into `self._content`.
  3. **`write_to_disk()`**

     * If `persist=True`, open the file in **binary** mode and write `self._content`.
  4. **`to_native_object() -> object`** (abstract)

     * Convert raw bytes (`self._content`) into a Python object (string, dict, list, etc.).
  5. **`raw_from_native_object(obj) -> bytes`** (abstract)

     * Convert a Python object back into raw bytes.
  6. **`init(cxt)`**

     * Create a DTPS queue at `self.object_path` with the chosen `properties`.
     * If `_content is not NOTSET`, immediately publish the initial value (`to_native_object() → CBOR → publish`).
     * If the topic’s properties allow both read/write (or pushable), subscribe to updates. On each update, call `on_update()`.

* **`on_update(rd: RawData)`**

  * Called when a new payload arrives on DTPS.
  * Convert payload to the native Python object (`rd.get_as_native_object()`), then to raw bytes (`raw_from_native_object(...)`).
  * If bytes differ from existing `_content`, overwrite `_content` and `write_to_disk()`.

* **`drop()`**

  * If `droppable=True`, unsubscribe from DTPS, remove the queue, and delete the file from disk.

### PlainFileAdapter

* **Purpose**: Treat file contents as a raw UTF-8 string (no further parsing).
* **`to_native_object()`**:

  ```python
  return self._content.decode("utf-8")
  ```
* **`raw_from_native_object(obj: str)`**:

  ```python
  return obj.encode("utf-8")
  ```

Use this for any file where the entire contents are a single string (e.g. `robot_type`, `permissions/camera`, etc.).

### YAMLFileAdapter

* **Purpose**: Treat file contents as YAML.
* **`to_native_object()`**:

  ```python
  return yaml.safe_load(self._content.decode("utf-8"))
  ```

  * If the file is completely empty or unparsable, `safe_load` may return `None` or raise.
* **`raw_from_native_object(obj)`**:

  ```python
  return yaml.dump(obj, sort_keys=True).encode("utf-8")
  ```

  * Always writes YAML with sorted keys.

Use this for any file ending in `.yaml` whose contents are structured as a YAML document (dictionaries, lists, primitives).

### JSONFileAdapter

* **Purpose**: Treat file contents as JSON. (Not used by default patterns, but included for completeness.)
* **`to_native_object()`**:

  ```python
  return json.loads(self._content.decode("utf-8"))
  ```
* **`raw_from_native_object(obj)`**:

  ```python
  return json.dumps(obj, sort_keys=True, indent=4).encode("utf-8")
  ```

  * Always writes JSON with 4-space indentation and sorted keys.

If you wish to support `.json` files under `/data/config`, simply add a regex entry in `ADAPTED_FILES` that points to `JSONFileAdapter`.

---

## How It Works

### Startup & Scanning `/data/config`

1. When the node starts, it instantiates `KVStore(parsed_args)`.
2. In `KVStore.__init__(...)`:

   * It collects a list of **all files** under `/data/config` (recursively).
   * For each regex in `ADAPTED_FILES` (in insertion order), it checks every file path:

     * If a file path matches the regex, it extracts named groups (`groupdict()`), builds the corresponding `object_path`, and instantiates the appropriate adapter (`PlainFileAdapter`, `YAMLFileAdapter`, etc.) with:

       * `file_path` = the absolute path on disk.
       * `object_path` = DTPS topic (e.g. `data/node/foo/config`).
       * `properties = adapter_template.properties` (often `FullRW` or `TopicProperties.readonly()`).
       * `droppable = adapter_template.droppable`.
       * `persist = True`.
       * If the regex had a `default` entry and the file did not exist on disk, it uses `create=True` and `default=…` to seed the file.
   * All created adapters are stored in `self._adapters[object_path]`.

### Adapter Instantiation & Lifecycle

Once `GenericFileAdapter` (or subclass) is constructed:

1. **`__post_init__`** checks for file existence.

   * If missing and `create=False`, it raises.
   * Otherwise, it reads existing content or writes initial/default content.
   * Immediately strips any leading/trailing whitespace or `\n`.
   * Re‐serializes via `to_native_object()` → `raw_from_native_object()` to normalize formatting.

2. During `KVStore.run()` (called by `asyncio.run(...)`):

   * Every adapter’s `.init(self._cxt)` is awaited:

     * It calls `DTPSContext.navigate(object_path).queue_create(...)`.
     * Publishes the initial content.
     * Subscribes to updates if `properties` allow writes.

3. When a DTPS client publishes a new payload on that topic:

   * `on_update()` is triggered. It converts the payload to raw bytes and, if changed, writes it back to the file.

### “define” RPC (Create/Update)

* **Topic**: `data/define` (under the same DTPS root).

* **Expected Payload**:

  ```json
  {
    "key": "/some/key/",
    "value": <any JSON/YAML‐serializable object>,
    "persist": <bool>    // optional; default = False
  }
  ```

  * `key` is a string like `"network/settings"` (leading/trailing “/” trimmed).
  * `value` is any JSON/YAML object or primitive.
  * `persist` controls whether the resulting file on disk should be left after “drop()” or not.

* **Behaviour**:

  1. Strips slashes, checks for “..” (disallowed).
  2. If `key == "example/key"` (hardcoded), returns a canned example payload.
  3. Otherwise, computes:

     ```python
     fpath = f"/data/config/{key}.yaml"
     object_path = f"data/{key}"
     ```
  4. If `object_path` is not already in `self._adapters`:

     * Instantiates a new `YAMLFileAdapter`, passing:

       * `file_path = fpath`
       * `object_path = f"data/{key}"`
       * `properties = FullRW`
       * `create = True`
       * `persist = (payload["persist"])`
       * `initial = payload["value"]`
       * `default = payload.get("default", NOTSET)`
     * Calls `adapter.set_content_quietly(value)` (writes the file immediately).
     * Calls `await adapter.init(self._cxt)` to create & publish the new topic.
  5. If it already exists, this RPC does nothing (no update).
  6. Returns a JSON “example” response confirming success.

### “drop” RPC (Delete)

* **Topic**: `data/drop`
* **Expected Payload**:

  ```json
  { "key": "/some/key/" }
  ```
* **Behaviour**:

  1. Strips slashes, checks for “..” (disallowed).
  2. If `key == "example/key"`, returns canned response.
  3. Otherwise, computes:

     ```python
     object_path = f"data/{key}"
     ```
  4. If that `object_path` is not in `self._adapters`, returns a 400 error.
  5. Otherwise:

     * Removes the adapter from `self._adapters`.
     * Calls `await adapter.drop()`, which:

       * If `droppable = True`, unsubscribes from DTPS, removes the queue, and deletes the file from disk.
       * If `droppable = False`, leaves the file intact and only removes the DTPS subscription.
  6. Returns a JSON example payload confirming deletion.

### Handling New/Removed Topics

* The node also subscribes to `dtps/topic_list` (via `self._cxt.navigate("dtps/topic_list").subscribe(self._on_topics_change)`).
* Whenever any new topic appears under `data/…`, `_on_topics_change()` is called:

  1. It loops through each topic string (e.g. `data/foo/bar`).
  2. If it starts with `data/`, it strips that prefix → `key = foo/bar`.
  3. Computes `fpath = /data/config/foo/bar.yaml`.
  4. If `object_path` is not in `self._adapters`:

     * Fetches metadata (`cxt.data_get()`) to obtain `app_data` fields such as:

       * `kvstore.persist` (bool)
       * `kvstore.initial` (value)
       * `kvstore.default` (value)
     * Creates a `YAMLFileAdapter` with `create=True`, `persist=persist`, `initial=value`, and `default=default`.
     * Calls `.set_content_quietly(value)` to write initial content, then `.init(self._cxt)`.
  5. If a previously‐tracked `object_path` is no longer in the topic list, it calls that adapter’s `.drop()`.

This mechanism ensures that **any new topic created by another client** will automatically produce a corresponding file under `/data/config`, and any removed topic will delete its file if appropriate.

### UDP Responder

At the very end of `KVStore.run()`, the node creates a `UDPResponder` on `0.0.0.0:11411` (or whatever `UDP_RESPONDER_HOST/PORT` is set to). The `UDPResponder` implementation is not shown here, but typically it:

* Listens for simple “ping” or “discovery” messages over UDP.
* Replies with node metadata (hostname, node type, etc.).

This allows other Duckietown components (or CLI tools) to quickly discover which DTPS‐nodes are running and on which ports.

---

## Content of the Files

Below is a summary of what each file is expected to contain, and how it is seen by the adapters:

1. **Plain‐Text Files (PlainFileAdapter)**

   * **Location Example**:

     * `/data/config/robot_type`
     * `/data/config/permissions/camera`
   * **Content**: A single UTF-8 string (no indentation or structured data). For example:

     ```
     duckiebot
     ```
   * **Published as**:

     ```python
     to_native_object() -> str  # e.g. "duckiebot"
     ```
   * **On‐disk format**: Exactly the raw string (no trailing newline, since `GenericFileAdapter` strips whitespace on read). If updated at runtime (DTPS write), the new string is written verbatim back to disk.

2. **YAML Files (YAMLFileAdapter)**

   * **Location Examples**:

     * `/data/config/node/<key>/<ROBOT_NAME>.yaml`
     * `/data/config/calibrations/<key>/<ROBOT_NAME>.yaml`
     * `/data/config/calibrations/<key>/default.yaml`
     * `/data/config/<key>.yaml`  (catch‐all)
   * **Content**: Any valid YAML document (mapping, sequence, or primitive). For instance:

     ```yaml
     # Example: /data/config/calibrations/camera_intrinsic/duckiebot1.yaml
     camera_matrix:
       fx: 628.2
       fy: 629.4
       cx: 322.3
       cy: 235.1
     distortion_coeffs:
       - -0.2
       - 0.1
       - 0.0
       - 0.0
     ```
   * **Published as**:

     ```python
     to_native_object() -> Python data structure (dict, list, string, number, etc.)
     ```
   * **On‐disk format**:

     * Stripped of leading/trailing whitespace (via `.strip()` in `GenericFileAdapter`).
     * Re‐serialized by `yaml.dump(obj, sort_keys=True)`, so keys are sorted alphabetically and a trailing newline is guaranteed (standard YAML style).

3. **JSON Files (JSONFileAdapter)**

   * **Location**:

     * Not included by default; you must add a regex entry for `*.json`.
   * **Content**: Standard JSON (objects, arrays, or primitives). For example:

     ```json
     {
         "threshold": 0.5,
         "modes": ["auto", "manual"],
         "enabled": true
     }
     ```
   * **Published as**:

     ```python
     to_native_object() -> Python data structure (dict, list, string, etc.)
     ```
   * **On‐disk format**:

     * Stripped of outer whitespace.
     * Re‐serialized by `json.dumps(obj, sort_keys=True, indent=4)`.

---

## Usage Examples

Below are a few common usage patterns. We assume you have already built and launched this node in your Duckietown environment, and that it is listening on host `0.0.0.0:11411`.

### 1. Reading an Existing YAML Configuration

Suppose you have on disk:

```
/data/config/network.yaml
```

with contents:

```yaml
ssid: MyWiFi
password: SuperSecret
```

At startup, the node:

1. Matches `network.yaml` against the catch-all `(?P<key>.*).yaml` pattern.
2. Instantiates `YAMLFileAdapter(file_path="/data/config/network.yaml", object_path="data/network", …)`.
3. In `__post_init__`, reads the file, strips whitespace, and re‐dumps it as normalized YAML.
4. Publishes this object on the DTPS topic `data/network`:

   ```json
   {
     "ssid": "MyWiFi",
     "password": "SuperSecret"
   }
   ```
5. Any DTPS client can now subscribe to `data/network` and receive that dictionary as a Python object.

### 2. Updating a Plain‐Text File (`robot_type`)

On disk:

```
/data/config/robot_type
```

contains:

```
duckiebot
```

Startup binds this file to topic `data/robot/type` via `PlainFileAdapter`. Suppose you want to change the robot type at runtime:

```python
# Example Python (pseudocode) to publish a new robot type:
client.publish("data/robot/type", RawData.cbor_from_native_object("duckiebot_mini"))
```

* The adapter’s `on_update()` is triggered, sees that `"duckiebot_mini"` differs from `"duckiebot"`, and writes the new bytes back to `/data/config/robot_type`.
* If you later open that file, it now reads:

  ```
  duckiebot_mini
  ```

### 3. Creating a New Configuration via “define”

Imagine you want to add a new “feature toggles” YAML file at `/data/config/features.yaml` with initial fields. Send the following to the `data/define` topic:

```json
{
  "key": "/features/",
  "value": {
    "enable_camera": true,
    "enable_lidar": false
  },
  "persist": true
}
```

* The node strips slashes → `key = "features"`.
* Creates a new file `/data/config/features.yaml` (because `.yaml` is appended).
* Writes:

  ```yaml
  enable_camera: true
  enable_lidar: false
  ```
* Publishes that YAML object on `data/features`.
* Any subscriber to `data/features` will immediately see:

  ```python
  {"enable_camera": True, "enable_lidar": False}
  ```

### 4. Deleting a Configuration via “drop”

To remove that same file and topic:

```json
{ "key": "/features/" }
```

published on `data/drop`.

* The node sees `object_path = "data/features"`, unsubscribes, removes the DTPS queue, and deletes `/data/config/features.yaml` from disk (because `droppable=True` for the catch-all YAML pattern).

---

## Environment & Logging

* By default, the node listens on **host** `0.0.0.0` and **port** `11411` for DTPS RPCs and topic creation:

  ```bash
  python3 kvstore_node.py --host 0.0.0.0 --port 11411
  ```
* A UDP responder is also started on `0.0.0.0:11411`.
* Logging is configured via:

  ```python
  install_colored_logs(level=logging.INFO)
  install_colored_logs(logger=logger)
  ```

  If you set the environment variable `DEBUG=1` (or `yes`/`true`), the logger’s level becomes `DEBUG`:

  ```bash
  export DEBUG=1
  python3 kvstore_node.py
  ```

  You will then see verbose logs about file matches, topic creations, updates, etc.
