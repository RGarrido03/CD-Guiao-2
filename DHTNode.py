""" Chord DHT node implementation. """

import socket
import threading
import logging
import pickle
import math
from typing import Optional, Any

from utils import dht_hash, contains

Address = tuple[str, int]
Args = dict[str, Any]


class FingerTable:
    """Finger Table."""

    def __init__(self, node_id: int, node_addr: Address, m_bits: int = 10):
        """Initialize Finger Table."""
        self.node_id = node_id
        self.node_addr = node_addr
        self.m_bits = m_bits

        # key: 1, ..., m_bits
        # value: (node_id, address)
        self.finger_table: dict[int, tuple[int, Address]] = {
            i: ((node_id + 2 ** (i - 1)) % (2**m_bits), node_addr)
            for i in range(1, m_bits + 1)
        }

    def fill(self, node_id: int, node_addr: Address) -> None:
        """Fill all entries of finger_table with node_id, node_addr."""
        self.finger_table = {i: (node_id, node_addr) for i in range(1, self.m_bits + 1)}

    def update(self, index: int, node_id: int, node_addr: Address) -> None:
        """Update index of table with node_id and node_addr."""
        self.finger_table[index] = (node_id, node_addr)

    def find(self, identification: int) -> Address:
        """Get node address of the closest preceding node (in finger table) of identification."""
        for i in self.finger_table.keys():
            if self.finger_table[i][0] >= identification:
                return (
                    self.finger_table[i - 1][1]
                    if i - 1 > 0
                    else self.finger_table[i][1]
                )
        return self.finger_table[1][1]

    def refresh(self) -> list[tuple[int, int, Address]]:
        """Retrieve finger table entries requiring refresh."""
        new_finger_table = {
            i: (
                (self.node_id + 2 ** (i - 1)) % (2**self.m_bits),
                self.finger_table[i][1],
            )
            for i in range(1, self.m_bits + 1)
        }

        self.finger_table = new_finger_table

        return [(idx, id, addr) for idx, (id, addr) in self.finger_table.items()]

    def getIdxFromId(self, id: int) -> int:
        """Get index of finger table entry corresponding to id."""
        # Find the index of the finger table entry that is closest to id
        offset = 2**self.m_bits if id < self.node_id else 0
        return int(math.log2(id - self.node_id + offset) + 1)

    def __repr__(self) -> str:
        return str(self.as_list)

    @property
    def as_list(self) -> list[tuple[int, Address]]:
        """return the finger table as a list of tuples: (identifier, (host, port)).
        NOTE: list index 0 corresponds to finger_table index 1
        """
        return list(self.finger_table.values())


class DHTNode(threading.Thread):
    """DHT Node Agent."""

    def __init__(
        self,
        address: Address,
        dht_address: Address = None,
        timeout: int = 3,
    ):
        """Constructor

        Parameters:
            address: self's address
            dht_address: address of a node in the DHT
            timeout: impacts how often stabilize algorithm is carried out
        """
        threading.Thread.__init__(self)
        self.done = False
        self.identification = dht_hash(address.__str__())
        self.addr = address  # My address
        self.dht_address = dht_address  # Address of the initial Node
        if dht_address is None:
            self.inside_dht = True
            # I'm my own successor
            self.successor_id = self.identification
            self.successor_addr = address
            self.predecessor_id = None
            self.predecessor_addr = None
        else:
            self.inside_dht = False
            self.successor_id = None
            self.successor_addr = None
            self.predecessor_id = None
            self.predecessor_addr = None

        self.finger_table = FingerTable(self.identification, address)

        self.keystore: dict[int, Any] = {}  # Where all data is stored
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(timeout)
        self.logger = logging.getLogger("Node {}".format(self.identification))

    def send(self, address: Address, msg: Any) -> None:
        """Send msg to address."""
        payload = pickle.dumps(msg)
        self.socket.sendto(payload, address)

    def recv(self) -> tuple[Optional[bytes], Optional[Address]]:
        """Retrieve msg payload and from address."""
        try:
            payload, addr = self.socket.recvfrom(1024)
        except socket.timeout:
            return None, None

        if len(payload) == 0:
            return None, addr
        return payload, addr

    def node_join(self, args: Args) -> None:
        """Process JOIN_REQ message.

        Parameters:
            args (dict): addr and id of the node trying to join
        """

        self.logger.debug("Node join: %s", args)
        addr: Address = args["addr"]
        identification: int = args["id"]
        if self.identification == self.successor_id:  # I'm the only node in the DHT
            self.successor_id = identification
            self.successor_addr = addr
            # [DONE] TODO update finger table index
            self.finger_table.fill(identification, addr)
            args = {"successor_id": self.identification, "successor_addr": self.addr}
            self.send(addr, {"method": "JOIN_REP", "args": args})
        elif contains(self.identification, self.successor_id, identification):
            args = {
                "successor_id": self.successor_id,
                "successor_addr": self.successor_addr,
            }
            self.successor_id = identification
            self.successor_addr = addr
            # TODO update finger table
            self.finger_table.fill(identification, addr)
            self.send(addr, {"method": "JOIN_REP", "args": args})
        else:
            self.logger.debug("Find Successor(%d)", args["id"])
            self.send(self.successor_addr, {"method": "JOIN_REQ", "args": args})
        self.logger.info(self)

    def get_successor(self, args: Args) -> None:
        """Process SUCCESSOR message.

        Parameters:
            args (dict): addr and id of the node asking
        """
        self.logger.debug("Get successor: %s", args)

        if contains(self.identification, self.successor_id, args["id"]):
            self.send(
                args["from"],
                {
                    "method": "SUCCESSOR_REP",
                    "args": {
                        "req_id": args["id"],
                        "successor_id": self.successor_id,
                        "successor_addr": self.successor_addr,
                    },
                },
            )
            return

        self.send(
            self.finger_table.find(args["id"]),
            {
                "method": "SUCCESSOR",
                "args": {"id": args["id"], "from": args["from"]},
            },
        )

    def notify(self, args: Args) -> None:
        """Process NOTIFY message.
            Updates predecessor pointers.

        Parameters:
            args (dict): id and addr of the predecessor node
        """

        self.logger.debug("Notify: %s", args)
        if self.predecessor_id is None or contains(
            self.predecessor_id, self.identification, args["predecessor_id"]
        ):
            self.predecessor_id = args["predecessor_id"]
            self.predecessor_addr = args["predecessor_addr"]
        self.logger.info(self)

    def stabilize(self, from_id: int, addr: Address) -> None:
        """Process STABILIZE protocol.
            Updates all successor pointers.

        Parameters:
            from_id: id of the predecessor of node with address addr
            addr: address of the node sending stabilize message
        """

        self.logger.debug("Stabilize: %s %s", from_id, addr)
        if from_id is not None and contains(
            self.identification, self.successor_id, from_id
        ):
            # Update our successor
            self.successor_id = from_id
            self.successor_addr = addr
            # [DONE] TODO update finger table
            self.finger_table.update(
                self.finger_table.getIdxFromId(from_id), from_id, addr
            )

        # notify successor of our existence, so it can update its predecessor record
        args = {"predecessor_id": self.identification, "predecessor_addr": self.addr}
        self.send(self.successor_addr, {"method": "NOTIFY", "args": args})

        # [DONE] TODO refresh finger_table
        for idx, id, addr in self.finger_table.refresh():
            self.send(
                addr,
                {"method": "SUCCESSOR", "args": {"id": id, "from": self.addr}},
            )

    def put(self, key: Any, value: Any, address: Address) -> None:
        """Store value in DHT.

        Parameters:
        key: key of the data
        value: data to be stored
        address: address where to send ack/nack
        """
        key_hash = dht_hash(key)
        self.logger.debug("Put: %s %s", key, key_hash)

        if contains(self.predecessor_id, self.identification, key_hash):
            self.logger.debug(
                'Storing "%s" (hash %s) in %s', key, key_hash, self.identification
            )
            self.keystore[key] = value
            self.send(address, {"method": "ACK"})
            return

        self.logger.debug("Forwarding %s from %s", key_hash, self.identification)
        self.send(
            self.finger_table.find(key_hash),
            {
                "method": "PUT",
                "args": {"key": key, "value": value, "from": address},
            },
        )

    def get(self, key: Any, address: Address) -> None:
        """Retrieve value from DHT.

        Parameters:
        key: key of the data
        address: address where to send ack/nack
        """
        key_hash = dht_hash(key)
        self.logger.debug("Get: %s %s", key, key_hash)

        if contains(self.predecessor_id, self.identification, key_hash):
            self.logger.debug(
                'Retrieving "%s" (hash %s) from %s', key, key_hash, self.identification
            )
            value = self.keystore[key]

            if value is None:
                self.send(address, {"method": "NACK"})
                return

            self.send(address, {"method": "ACK", "args": value})
            return

        self.logger.debug("Forwarding get %s from %s", key_hash, self.identification)
        self.send(
            self.finger_table.find(key_hash),
            {"method": "GET", "args": {"key": key, "from": address}},
        )

    def process_successor_rep(self, args: Args) -> None:
        """Process SUCCESSOR_REP message.

        Get the successor of the node with id req_id and update the finger table.

        Parameters:
        args: req_id, successor_id, successor_addr
        """
        self.logger.debug("Process SUCCESSOR_REP message: %s", args)

        idx = self.finger_table.getIdxFromId(args["req_id"])
        self.finger_table.update(idx, args["successor_id"], args["successor_addr"])

    def run(self) -> None:
        self.socket.bind(self.addr)

        # Loop untiln joining the DHT
        while not self.inside_dht:
            join_msg = {
                "method": "JOIN_REQ",
                "args": {"addr": self.addr, "id": self.identification},
            }
            self.send(self.dht_address, join_msg)
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                self.logger.debug("O: %s", output)
                if output["method"] == "JOIN_REP":
                    args = output["args"]
                    self.successor_id = args["successor_id"]
                    self.successor_addr = args["successor_addr"]
                    # [DONE] TODO fill finger table
                    self.finger_table.fill(self.successor_id, self.successor_addr)
                    self.inside_dht = True
                    self.logger.info(self)

        while not self.done:
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                self.logger.info("O: %s", output)
                if output["method"] == "JOIN_REQ":
                    self.node_join(output["args"])
                elif output["method"] == "NOTIFY":
                    self.notify(output["args"])
                elif output["method"] == "PUT":
                    self.put(
                        output["args"]["key"],
                        output["args"]["value"],
                        output["args"].get("from", addr),
                    )
                elif output["method"] == "GET":
                    self.get(output["args"]["key"], output["args"].get("from", addr))
                elif output["method"] == "PREDECESSOR":
                    # Reply with predecessor id
                    self.send(
                        addr, {"method": "STABILIZE", "args": self.predecessor_id}
                    )
                elif output["method"] == "SUCCESSOR":
                    # Reply with successor of id
                    self.get_successor(output["args"])
                elif output["method"] == "STABILIZE":
                    # Initiate stabilize protocol
                    self.stabilize(output["args"], addr)
                elif output["method"] == "SUCCESSOR_REP":
                    # [DONE] TODO Implement processing of SUCCESSOR_REP
                    self.process_successor_rep(output["args"])
            else:  # timeout occurred, lets run the stabilize algorithm
                # Ask successor for predecessor, to start the stabilize process
                self.send(self.successor_addr, {"method": "PREDECESSOR"})

    def __str__(self):
        return "Node ID: {}; DHT: {}; Successor: {}; Predecessor: {}; FingerTable: {}".format(
            self.identification,
            self.inside_dht,
            self.successor_id,
            self.predecessor_id,
            self.finger_table,
        )

    def __repr__(self):
        return self.__str__()
