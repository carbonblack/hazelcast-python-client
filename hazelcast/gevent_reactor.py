import gevent
import errno
import logging
import socket
import threading
import time

from hazelcast.connection import Connection, BUFFER_SIZE
from hazelcast.exception import HazelcastError


class GeventReactor(object):
    _thread = None
    _is_live = False
    logger = logging.getLogger("Reactor")

    def __init__(self):
        self._connections = set()

    def start(self):
        self._is_live = True

    @staticmethod
    def add_timer_absolute(timeout, callback):
        return gevent.spawn_later(timeout - time.time(), callback)

    @staticmethod
    def add_timer(delay, callback):
        return gevent.spawn_later(delay, callback)

    def shutdown(self):
        for connection in self._connections:
            try:
                self.logger.debug("Shutdown connection: %s", str(connection))
                connection.close(HazelcastError("Client is shutting down"))
            except OSError, err:
                if err.args[0] == socket.EBADF:
                    pass
                else:
                    raise
        self._is_live = False

    def new_connection(self, address, connect_timeout, socket_options, connection_closed_callback, message_callback):
        conn = GeventConnection(address, connect_timeout, socket_options, connection_closed_callback,
                                message_callback)
        self._connections.add(conn)
        return conn


class GeventConnection(Connection):
    sent_protocol_bytes = False
    logger = logging.getLogger("GeventConnection")

    def __init__(self, address, connect_timeout, socket_options, connection_closed_callback, message_callback):
        Connection.__init__(self, address, connection_closed_callback, message_callback)

        self._write_lock = threading.Lock()
        self._socket = gevent.socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(connect_timeout)

        # set tcp no delay
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # set socket buffer
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_SIZE)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)

        for socket_option in socket_options:
            self._socket.setsockopt(socket_option.level, socket_option.option, socket_option.value)

        self._socket.connect(self._address)
        self.logger.debug("Connected to %s", self._address)

        # Set no timeout as we use seperate greenlets to handle heartbeat timeouts, etc
        self._socket.settimeout(None)

        self.write("CB2")
        self.sent_protocol_bytes = True

        self._read_thread = gevent.spawn(self._read_loop)

    def _read_loop(self):
        while not self._closed:
            try:
                self._read_buffer += self._socket.recv(BUFFER_SIZE)
                if self._read_buffer:
                    self.receive_message()
                else:
                    self.close(IOError("Connection closed by server."))
                    return
            except socket.error, e:
                #if e.args[0] != errno.EAGAIN and e.args[0] != errno.EDEADLK:
                if e.args[0] != errno.EAGAIN:
                    self.logger.exception("Received error")
                    self.close(IOError(e))
                    return

    def readable(self):
        return not self._closed and self.sent_protocol_bytes

    def write(self, data):
        # if write queue is empty, send the data right away, otherwise add to queue
        with self._write_lock:
            self._socket.sendall(data)

    def close(self, cause):
        if not self._closed:
            self._closed = True
            self._socket.close()
            self._connection_closed_callback(self, cause)
