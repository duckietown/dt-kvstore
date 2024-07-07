import argparse
import asyncio
import dataclasses
import json
import os
import re
from abc import abstractmethod
from typing import Optional, Dict, Type, Union, Set, List, cast, Any

import yaml

from dt_cli_utils import install_colored_logs
from dt_robot_utils import get_robot_name
from dtps import context, DTPSContext, SubscriptionInterface
from dtps_http import TopicProperties, RawData, TransformError

import logging

from .udp_responder import UDPResponder

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if "DEBUG" in os.environ and os.environ["DEBUG"].lower() in ["1", "yes", "true"]:
    logger.setLevel(logging.DEBUG)

# install colored logs on all loggers and set the default level to INFO
install_colored_logs(level=logging.INFO)
# install colored logs on our logger with our custom level
install_colored_logs(logger=logger)

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 11411

UDP_RESPONDER_HOST = "0.0.0.0"
UDP_RESPONDER_PORT = 11411

ROBOT_NAME: str = get_robot_name()
EXAMPLE_CREATE_PAYLOAD = {"key": "/example/key/", "value": ["duckietown", "is", "cool"], "persist": False}
EXAMPLE_DROP_PAYLOAD = {"key": "/example/key/"}

FullRW = None
AUTO = None
NOTSET = object()
ObjectPath = str


@dataclasses.dataclass
class FileAdapterTemplate:
    object_path: str
    kind: Type["GenericFileAdapter"]
    droppable: bool
    properties: Optional[TopicProperties] = AUTO


@dataclasses.dataclass
class GenericFileAdapter:
    file_path: str
    object_path: str
    properties: Optional[TopicProperties]
    persist: bool
    droppable: bool
    create: bool = False
    initial: Optional[object] = NOTSET

    _content: Optional[bytes] = AUTO
    _context: DTPSContext = None
    _subscriber: SubscriptionInterface = None
    
    def __post_init__(self):
        if not os.path.exists(self.file_path):
            if not self.create:
                raise FileNotFoundError(f"File not found: {self.file_path}")
            # make sure we received data
            if self.initial is NOTSET:
                raise ValueError("When creating a new file, 'initial' must be provided")
            # create the directory
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            # create the file
            self._content = self.raw_from_native_object(self.initial)
            self.write_to_disk()
        # read the content from disk
        self.read_from_disk()
        # format the content
        self._content = self.raw_from_native_object(self.to_native_object())
        
    def read_from_disk(self):
        if not self.persist:
            return
        with open(self.file_path, "rb") as fin:
            self._content = fin.read()

    def write_to_disk(self):
        if not self.persist:
            return
        with open(self.file_path, "wb") as fout:
            fout.write(self._content)

    def set_content_quietly(self, content: object):
        self._content = self.raw_from_native_object(content)
        self.write_to_disk()

    @abstractmethod
    def to_native_object(self) -> object:
        pass

    @abstractmethod
    def raw_from_native_object(self, obj: Union[object, None]) -> bytes:
        pass

    async def on_update(self, rd: RawData):
        # update the content
        new_content: bytes = self.raw_from_native_object(rd.get_as_native_object())
        # check if the content has changed
        if new_content == self._content:
            return
        # update the content
        self._content = new_content
        # write new content to disk
        self.write_to_disk()

    async def init(self, cxt: DTPSContext):
        self._context = await cxt.navigate(self.object_path).queue_create(topic_properties=self.properties)
        # publish the initial value
        await self._context.publish(RawData.cbor_from_native_object(self.to_native_object()))
        # subscribe for updates
        if self.properties is FullRW or self.properties.pushable:
            self._subscriber = await self._context.subscribe(self.on_update)

    async def drop(self) -> bool:
        if not self.droppable:
            return False
        if self._subscriber:
            await self._subscriber.unsubscribe()
        await self._context.remove()
        if self.persist:
            os.remove(self.file_path)

    def to_rawdata(self) -> RawData:
        return RawData.json_from_native_object(self.to_native_object())

    def __str__(self):
        json_str = json.dumps(dataclasses.asdict(self), sort_keys=True)
        return f"{self.__class__.__name__}({json_str[1:-1]})"


@dataclasses.dataclass
class JSONFileAdapter(GenericFileAdapter):

    def to_native_object(self) -> Union[dict, list, str, int, float, bool, None]:
        return json.loads(self._content.decode("utf-8"))

    def raw_from_native_object(self, obj: Union[dict, list, str, int, float, bool, None]) -> bytes:
        return json.dumps(obj, sort_keys=True, indent=4).encode("utf-8")


@dataclasses.dataclass
class YAMLFileAdapter(GenericFileAdapter):

    def to_native_object(self) -> Union[dict, list, str, int, float, bool, bytes, None]:
        return yaml.safe_load(self._content.decode("utf-8"))

    def raw_from_native_object(self, obj: Union[dict, list, str, int, float, bool, bytes, None]) -> bytes:
        return yaml.dump(obj, sort_keys=True).encode("utf-8")


@dataclasses.dataclass
class PlainFileAdapter(GenericFileAdapter):

    def to_native_object(self) -> str:
        return self._content.decode("utf-8")

    def raw_from_native_object(self, obj: str) -> bytes:
        return obj.encode("utf-8")

    def to_rawdata(self) -> RawData:
        return RawData.simple_string(self.to_native_object())


ADAPTED_FILES_DIR = "/data/config"
ADAPTED_FILES = {
    f"{ADAPTED_FILES_DIR}/node/(?P<key>.*)/{ROBOT_NAME}.yaml": FileAdapterTemplate(
        object_path="data/node/{key}/config",
        properties=FullRW,
        kind=YAMLFileAdapter,
        droppable=True,
    ),

    f"{ADAPTED_FILES_DIR}/permissions/(?P<key>.*)": FileAdapterTemplate(
        object_path="data/permission/{key}",
        properties=FullRW,
        kind=PlainFileAdapter,
        droppable=False,
    ),

    f"{ADAPTED_FILES_DIR}/calibrations/(?P<key>.*)/{ROBOT_NAME}.yaml": FileAdapterTemplate(
        object_path="data/calibration/{key}/current",
        properties=FullRW,
        kind=YAMLFileAdapter,
        droppable=True,
    ),

    f"{ADAPTED_FILES_DIR}/calibrations/(?P<key>.*)/default.yaml": FileAdapterTemplate(
        object_path="data/calibration/{key}/default",
        properties=TopicProperties.readonly(),
        kind=YAMLFileAdapter,
        droppable=False,
    ),

    f"{ADAPTED_FILES_DIR}/robot_(?P<key>.*)": FileAdapterTemplate(
        object_path="data/robot/{key}",
        properties=TopicProperties.readonly(),
        kind=PlainFileAdapter,
        droppable=False,
    ),

    # match any other YAML file (always leave this as the last item in this dictionary)
    f"{ADAPTED_FILES_DIR}/(?P<key>.*).yaml": FileAdapterTemplate(
        object_path="data/{key}",
        properties=FullRW,
        kind=YAMLFileAdapter,
        droppable=True,
    ),
}


class KVStore:

    def __init__(self, args: argparse.Namespace):
        self._args: argparse.Namespace = args
        self._cxt: Optional[DTPSContext] = None
        self._adapters: Dict[ObjectPath, GenericFileAdapter] = {}
        # all files
        files = [os.path.join(dp, f) for dp, dn, fn in os.walk(ADAPTED_FILES_DIR) for f in fn]
        matched: Set[str] = set()
        # process regexed files
        for regex, adapter_template in ADAPTED_FILES.items():
            AdapterClass: Type[GenericFileAdapter] = adapter_template.kind
            pattern = re.compile(f"^{regex}$")
            # find all files matching the pattern
            for file in files:
                if file in matched:
                    continue
                match = pattern.match(file)
                if not match:
                    continue
                groups: Dict[str, str] = match.groupdict()
                # apply groups to the object path
                object_path = adapter_template.object_path.format(**groups)
                # create a new adapter
                # noinspection PyArgumentList
                adapter = AdapterClass(
                    file_path=file,
                    object_path=object_path,
                    properties=adapter_template.properties,
                    droppable=adapter_template.droppable,
                    persist=True,
                )
                self._adapters[object_path] = adapter
                matched.add(file)

    async def define(self, rd: RawData):
        # decode request
        data: object = rd.get_as_native_object()
        if not isinstance(data, dict) or "key" not in data or "value" not in data:
            return TransformError(400, f"Expected a payload of the form "
                                       f"'{{\"key\": \"<str>\", \"value\": \"<any>\", \"persist\": \"<bool>\"}}'")

        key: str = data["key"].strip("/")
        value: Union[dict, list, str, int, float, bool, bytes] = data["value"]
        persist: bool = data.get("persist", False)

        if ".." in key:
            return TransformError(400, "Key cannot contain '..'")

        # example key/value
        if key == EXAMPLE_CREATE_PAYLOAD["key"].strip("/"):
            return RawData.json_from_native_object(EXAMPLE_CREATE_PAYLOAD)

        fpath: str = f"{ADAPTED_FILES_DIR}/{key}.yaml"
        object_path: str = f"data/{key}"
        if object_path not in self._adapters:
            # create new adapter
            adapter = YAMLFileAdapter(
                file_path=fpath,
                object_path=f"data/{key}",
                properties=FullRW,
                create=True,
                persist=persist,
                initial=value,
                droppable=True,
            )
            adapter.set_content_quietly(value)
            self._adapters[object_path] = adapter
            # NOTE: it is important that we add the adapter to the dict before creating the queue
            await adapter.init(self._cxt)
        else:
            pass
        # ---
        return RawData.json_from_native_object(EXAMPLE_CREATE_PAYLOAD)

    async def drop(self, rd: RawData):
        # decode request
        data: object = rd.get_as_native_object()
        if not isinstance(data, dict) or "key" not in data:
            return TransformError(400, f"Expected a payload of the form '{{\"key\": \"<str>\"}}'")

        key: str = data["key"].strip("/")

        if ".." in key:
            return TransformError(400, "Key cannot contain '..'")

        # example key/value
        if key == EXAMPLE_DROP_PAYLOAD["key"].strip("/"):
            return RawData.json_from_native_object(EXAMPLE_DROP_PAYLOAD)

        object_path: str = f"data/{key}"
        if object_path not in self._adapters:
            return TransformError(400, f"Key '{key}' not found")
        else:
            # get adapter
            adapter = self._adapters[object_path]
            del self._adapters[object_path]
            # NOTE: it is important that we remove the adapter from the dict before dropping the context
            await adapter.drop()
        # ---
        return RawData.json_from_native_object(EXAMPLE_DROP_PAYLOAD)

    async def _on_topics_change(self, rd: RawData):
        topics: List[str] = cast(list, rd.get_as_native_object())
        topics = [t.strip("/") for t in topics]
        # process new topics
        for topic in topics:
            object_path: str = topic.strip("/")
            if not topic.startswith("data/"):
                continue
            # remove data/
            key: str = topic[5:].strip("/")
            fpath: str = f"{ADAPTED_FILES_DIR}/{key}.yaml"

            # create new adapter if we don't have one
            if object_path not in self._adapters:
                try:
                    cxt = self._cxt.navigate(object_path).meta()
                    rd: RawData = await cxt.data_get()
                except Exception as e:
                    logger.error(f"Error fetching metadata for '{object_path}': {e}")
                    continue
                # get metadata
                meta = cast(dict, rd.get_as_native_object())
                app_data = meta.get("topics", {}).get("", {}).get("app_data", {})
                # args
                persist: bool = app_data.get("kvstore.persist", False)
                value: Any = app_data.get("kvstore.initial", None)
                # create adapter
                adapter = YAMLFileAdapter(
                    file_path=fpath,
                    object_path=topic,
                    properties=FullRW,
                    create=True,
                    droppable=True,
                    persist=persist,
                    initial=value,
                )
                adapter.set_content_quietly(value)
                self._adapters[object_path] = adapter
                logger.info(f"Creating new queue for '{object_path}' with configuration: {app_data}")
                # NOTE: it is important that we add the adapter to the dict before creating the queue
                await adapter.init(self._cxt)
        # process removed topics
        for object_path in list(self._adapters.keys()):
            if object_path not in topics:
                adapter = self._adapters.pop(object_path)
                logger.info(f"Dropping queue for '{object_path}'")
                # NOTE: it is important that we remove the adapter from the dict before dropping the context
                await adapter.drop()

    async def run(self):
        self._cxt = await context("kvstore", urls=self.urls(self._args))
        # initialize all adapters
        for adapter in self._adapters.values():
            await adapter.init(self._cxt)
        # add 'define' rpc
        define = await self._cxt.navigate("define").queue_create(transform=self.define)
        await define.publish(RawData.json_from_native_object(EXAMPLE_CREATE_PAYLOAD))
        # add 'drop' rpc
        drop = await self._cxt.navigate("drop").queue_create(transform=self.drop)
        await drop.publish(RawData.json_from_native_object(EXAMPLE_DROP_PAYLOAD))
        # monitor new queues created
        await self._cxt.navigate("dtps/topic_list").subscribe(self._on_topics_change)
        # start UDP responder
        loop = asyncio.get_event_loop()
        await UDPResponder.create(loop, UDP_RESPONDER_HOST, UDP_RESPONDER_PORT)
        # keep running
        try:
            while True:
                await asyncio.sleep(1.0)
        except (KeyboardInterrupt, asyncio.exceptions.CancelledError):
            pass

    @staticmethod
    def urls(args: argparse.Namespace):
        return [
            f"create:http://{args.host}:{args.port}/",
            f"create:http+unix://%2Fdtps%2Fkvstore.sock/"
        ]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host to bind to")
    parser.add_argument("--port", default=DEFAULT_PORT, help="Port to bind to")
    parsed = parser.parse_args()
    # ---
    kvstore = KVStore(parsed)
    try:
        asyncio.run(kvstore.run())
    except (KeyboardInterrupt, asyncio.exceptions.CancelledError):
        pass
