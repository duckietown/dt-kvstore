import asyncio
import dataclasses
import json
import logging
import os
import socket
import traceback
from asyncio import DatagramTransport
from typing import Tuple

from dt_robot_utils import get_robot_name, get_robot_type, get_robot_configuration, get_robot_hardware

logger = logging.getLogger("udp-responder")
logger.setLevel(logging.INFO)
if "DEBUG" in os.environ and os.environ["DEBUG"].lower() in ["1", "yes", "true"]:
    logger.setLevel(logging.DEBUG)

IPAddress = str
Port = int


@dataclasses.dataclass
class Packet:
    version: str

    def asdict(self) -> dict:
        return dataclasses.asdict(self)

    def serialize(self) -> bytes:
        return json.dumps(dataclasses.asdict(self)).encode()

    @classmethod
    def deserialize(cls, data: bytes):
        try:
            return cls(**json.loads(data))
        except Exception:
            raise ValueError(f"Could not deserialize data: {data}")


@dataclasses.dataclass
class PingPacket(Packet):
    # port where the client is listening for pong packets
    port: Port


@dataclasses.dataclass
class PongPacket(Packet):
    name: str
    type: str
    configuration: str
    hardware: str


class UDPResponder(asyncio.DatagramProtocol):

    def __init__(self):
        self._sock: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def connection_made(self, transport: DatagramTransport):
        sock: socket.socket = transport.get_extra_info('socket')
        addr: Tuple[IPAddress, Port] = sock.getsockname()
        logger.info(f"Listening on {addr}...")

    def datagram_received(self, data: bytes, addr: Tuple[IPAddress, Port]):
        # decode the address
        host, _ = addr
        # decode the packet
        try:
            ping = PingPacket.deserialize(data)
            logger.debug(f"Received PING: Data(host={host}, payload={ping.asdict()})")
        except ValueError as e:
            logger.warning(e.args[0])
            return
        # formulate the response
        pong = PongPacket(
            version='1',
            name=get_robot_name(),
            type=get_robot_type().name.lower(),
            configuration=get_robot_configuration().name,
            hardware=get_robot_hardware().name.lower()
        )
        logger.debug(f"Sending PONG: Data(host={host}, payload={pong.asdict()})")
        # send the response
        try:
            self._sock.sendto(pong.serialize(), (host, ping.port))
        except Exception as e:
            traceback.print_exc()
            logger.warning(e)

    @classmethod
    async def create(cls, loop: asyncio.AbstractEventLoop, host: str, port: int) -> 'UDPResponder':
        _, udpresponder = await loop.create_datagram_endpoint(UDPResponder, local_addr=(host, port))
        return udpresponder
