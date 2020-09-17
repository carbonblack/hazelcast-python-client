try:
    import gevent
except ImportError:
    # Don't fail if we can't import gevent, setup.py ends up pulling in this module and we don't want
    # The bdist_wheel command to fail. If gevent is really missing we'll know soon enough after starting up.
    pass

import errno
import logging
import socket
import threading
import time

from hazelcast.config import PROTOCOL
from hazelcast.connection import Connection, BUFFER_SIZE
from hazelcast.exception import HazelcastError

try:
    import ssl
except ImportError:
    ssl = None


class GeventTimer(object):
    def __init__(self, delay, callback):
        self._greenlet = None
        self._callback = callback
        self._delay = delay
    
    def _on_callback(self):
        self._greenlet = None
        if self._callback:
            self._callback()
    
    def start(self):
        if self._greenlet:
            return
        self._greenlet = gevent.spawn_later(self._delay, self._on_callback)
    
    def cancel(self):
        if not self._greenlet:
            return
        self._greenlet.kill()
        self._greenlet = None
    

class GeventReactor(object):
    _thread = None
    _is_live = False
    logger = logging.getLogger("HazelcastClient.GeventReactor")

    def __init__(self, logger_extras=None):
        self._logger_extras = logger_extras
        self._connections = set()

    def start(self):
        self._is_live = True

    @staticmethod
    def add_timer_absolute(timeout, callback):
        return GeventReactor.add_timer(timeout - time.time(), callback)

    @staticmethod
    def add_timer(delay, callback):
        timer = GeventTimer(delay, callback)
        timer.start()
        return timer

    def shutdown(self):
        for connection in self._connections:
            try:
                self.logger.debug("Shutdown connection: %s", str(connection), extra=self._logger_extras)
                connection.close(HazelcastError("Client is shutting down"))
            except OSError as err:
                if err.args[0] == socket.EBADF:
                    pass
                else:
                    raise
        self._is_live = False

    def new_connection(self, address, connect_timeout, socket_options, connection_closed_callback, message_callback,
                       network_config):
        conn = GeventConnection(address, connect_timeout, socket_options, connection_closed_callback,
                                message_callback, network_config, self._logger_extras)
        self._connections.add(conn)
        return conn


class GeventConnection(Connection):
    sent_protocol_bytes = False
    read_buffer_size = BUFFER_SIZE

    def __init__(self, address, connect_timeout, socket_options, connection_closed_callback,
                 message_callback, network_config, logger_extras=None):
        Connection.__init__(self, address, connection_closed_callback, message_callback, logger_extras)

        self._write_lock = threading.Lock()
        self._socket = gevent.socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(connect_timeout)

        # set tcp no delay
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # set socket buffer
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_SIZE)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)

        for socket_option in socket_options:
            if socket_option.option is socket.SO_RCVBUF:
                self.read_buffer_size = socket_option.value

            self._socket.setsockopt(socket_option.level, socket_option.option, socket_option.value)

        self._socket.connect(self._address)
        self.start_time_in_seconds = time.time()
        self.logger.debug("Connected to %s", self._address, extra=self._logger_extras)

        ssl_config = network_config.ssl_config
        if ssl and ssl_config.enabled:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)

            protocol = ssl_config.protocol

            # Use only the configured protocol
            try:
                if protocol != PROTOCOL.SSLv2:
                    ssl_context.options |= ssl.OP_NO_SSLv2
                if protocol != PROTOCOL.SSLv3 and protocol != PROTOCOL.SSL:
                    ssl_context.options |= ssl.OP_NO_SSLv3
                if protocol != PROTOCOL.TLSv1:
                    ssl_context.options |= ssl.OP_NO_TLSv1
                if protocol != PROTOCOL.TLSv1_1:
                    ssl_context.options |= ssl.OP_NO_TLSv1_1
                if protocol != PROTOCOL.TLSv1_2 and protocol != PROTOCOL.TLS:
                    ssl_context.options |= ssl.OP_NO_TLSv1_2
                if protocol != PROTOCOL.TLSv1_3:
                    ssl_context.options |= ssl.OP_NO_TLSv1_3
            except AttributeError:
                pass

            ssl_context.verify_mode = ssl.CERT_REQUIRED

            if ssl_config.cafile:
                ssl_context.load_verify_locations(ssl_config.cafile)
            else:
                ssl_context.load_default_certs()

            if ssl_config.certfile:
                ssl_context.load_cert_chain(ssl_config.certfile, ssl_config.keyfile, ssl_config.password)

            if ssl_config.ciphers:
                ssl_context.set_ciphers(ssl_config.ciphers)

            self._socket = ssl_context.wrap_socket(self._socket)

        # Set no timeout as we use seperate greenlets to handle heartbeat timeouts, etc
        self._socket.settimeout(None)

        self.write(b"CB2")
        self.sent_protocol_bytes = True

        self._read_thread = gevent.spawn(self._read_loop)

    def _read_loop(self):
        while not self._closed:
            try:
                new_data = self._socket.recv(self.read_buffer_size)
                if new_data:
                    self._read_buffer.extend(new_data)
                    self.last_read_in_seconds = time.time()
                    self.receive_message()
                else:
                    self.close(IOError("Connection closed by server."))
                    return
            except socket.error as e:
                if e.args[0] != errno.EAGAIN:
                    self.logger.exception("Received error", extra=self._logger_extras)
                    self.close(IOError(e))
                    return

    def readable(self):
        return not self._closed and self.sent_protocol_bytes

    def write(self, data):
        # if write queue is empty, send the data right away, otherwise add to queue
        with self._write_lock:
            self._socket.sendall(data)
            self.last_write_in_seconds = time.time()

    def close(self, cause):
        if not self._closed:
            self._closed = True
            self._socket.close()
            self._connection_closed_callback(self, cause)
