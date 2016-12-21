import gevent
import errno
import logging
import socket
import threading
import time
from Queue import PriorityQueue

from hazelcast.connection import Connection, BUFFER_SIZE
from hazelcast.exception import HazelcastError
from hazelcast.future import Future


class GeventReactor(object):
    _thread = None
    _is_live = False
    logger = logging.getLogger("Reactor")

    def __init__(self):
        self._timers = PriorityQueue()
        self._connections = set()

    def start(self):
        self._is_live = True
        self._thread = gevent.spawn(self._loop)

    def _loop(self):
        self.logger.debug("Starting Reactor Thread")
        Future._threading_locals.is_reactor_thread = True  # pylint: disable=W0212
        while self._is_live:
            try:
                self._check_timers()
                gevent.sleep(0.1)
            except:
                self.logger.exception("Error in Reactor Thread")
                return
        self.logger.debug("Reactor Thread exited.")

    def _check_timers(self):
        now = time.time()
        while not self._timers.empty():
            try:
                _, timer = self._timers.queue[0]
            except IndexError:
                return

            if timer.check_timer(now):
                self._timers.get_nowait()
            else:
                return

    def add_timer_absolute(self, timeout, callback):
        timer = Timer(timeout, callback, self._cleanup_timer)
        self._timers.put_nowait((timer.end, timer))
        return timer

    def add_timer(self, delay, callback):
        return self.add_timer_absolute(delay + time.time(), callback)

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
        self._thread.join()

    def new_connection(self, address, connect_timeout, socket_options, connection_closed_callback, message_callback):
        conn = GeventConnection(address, connect_timeout, socket_options, connection_closed_callback,
                                  message_callback)
        self._connections.add(conn)
        return conn

    def _cleanup_timer(self, timer):
        try:
            self._timers.queue.remove((timer.end, timer))
        except ValueError:
            pass


class GeventConnection(Connection):
    sent_protocol_bytes = False

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

        # the socket should be non-blocking from now on
        self._socket.settimeout(0)

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

            gevent.sleep(0.1)

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


class Timer(object):
    canceled = False

    def __init__(self, end, timer_ended_cb, timer_canceled_cb):
        self.end = end
        self.timer_ended_cb = timer_ended_cb
        self.timer_canceled_cb = timer_canceled_cb

    def cancel(self):
        self.canceled = True
        self.timer_canceled_cb(self)

    def check_timer(self, now):
        if self.canceled:
            return True

        if now > self.end:
            self.timer_ended_cb()
            return True
