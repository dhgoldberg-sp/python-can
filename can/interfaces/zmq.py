
from typing import Any, Optional, Tuple
import can.typechecking

import time
import logging
import zmq

from can import BusABC, Message

# OK so there are already virtual interfaces, but here's another....

logger = logging.getLogger(__name__)

class ZmqBus(BusABC):

    _ANTI_LOOPBACK_TOPIC = b'log'

    def __init__(self,
        channel: Any,
        can_filters: Optional[can.typechecking.CanFilters] = None,
        **kwargs: object
    ) -> None:
        """
        tbd
        """

        if not channel:
            raise TypeError("Must specify a host address:port.")

        (tx_url, rx_url) = channel.split("+")

        ctx = zmq.Context.instance()
        self.pub = ctx.socket(zmq.PUB)
        self.pub.connect(tx_url)
        self.tx_poller = zmq.Poller()
        self.tx_poller.register(self.pub, zmq.POLLOUT)

        self.sub = ctx.socket(zmq.SUB)
        self.sub.setsockopt_string(zmq.SUBSCRIBE, "")
        self.sub.connect(rx_url)
        self.rx_poller = zmq.Poller()
        self.rx_poller.register(self.sub, zmq.POLLIN)

        super().__init__(channel, can_filters=can_filters, **kwargs)

    def _recv_internal(self, timeout: Optional[float]) -> Tuple[Optional[Message], bool]:
        timeout_ms = timeout * 1000 if timeout is not None else None
        events = dict(self.rx_poller.poll(timeout_ms))
        msg = None

        if len(events) != 0:
            # assuming it's the right event if one exists, but this is bad practice...
            (module, arb_id, can_data) = self.sub.recv_multipart()

            if module != self._ANTI_LOOPBACK_TOPIC:
                msg = Message(
                    timestamp=time.time(),  # close enough
                    arbitration_id=int(arb_id),  # wtf zmq
                    is_extended_id=False,
                    dlc=len(can_data),
                    data=can_data,
                )

        return msg, False

    def send(self, msg: Message, timeout: Optional[float]):
        timeout_ms = timeout * 1000 if timeout is not None else None
        events = dict(self.tx_poller.poll(timeout_ms))

        if len(events) != 0:
            # assuming it's the right event if one exists, but this is bad practice...

            # czmq serializes uint32's ("4") as ascii apparently.
            self.pub.send_multipart([self._ANTI_LOOPBACK_TOPIC, f"{msg.arbitration_id}".encode("ascii"), msg.data])

    def shutdown(self):
        self.pub.close()
        self.sub.close()
