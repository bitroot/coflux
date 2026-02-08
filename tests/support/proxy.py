"""Simple TCP proxy for simulating network drops in tests."""

import select
import socket
import threading


class TCPProxy:
    """Forwards TCP connections to a target, with the ability to kill all active connections.

    The proxy always accepts new connections. Call disconnect() to close all
    currently active connections (simulating a network drop) without stopping
    the proxy itself.
    """

    def __init__(self, target_host, target_port):
        self.target = (target_host, target_port)
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self.port = self._server.getsockname()[1]
        self._server.listen(5)
        self._connections = []  # list of (client_sock, upstream_sock)
        self._lock = threading.Lock()
        self._running = True
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def _accept_loop(self):
        while self._running:
            readable, _, _ = select.select([self._server], [], [], 0.5)
            if not readable:
                continue
            try:
                client, _ = self._server.accept()
                upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                upstream.connect(self.target)
                pair = (client, upstream)
                with self._lock:
                    self._connections.append(pair)
                threading.Thread(
                    target=self._forward, args=(client, upstream, pair), daemon=True
                ).start()
                threading.Thread(
                    target=self._forward, args=(upstream, client, pair), daemon=True
                ).start()
            except OSError:
                if not self._running:
                    break

    def _forward(self, src, dst, pair):
        try:
            while self._running:
                data = src.recv(8192)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            self._close_pair(pair)

    def _close_pair(self, pair):
        for sock in pair:
            try:
                sock.close()
            except OSError:
                pass
        with self._lock:
            try:
                self._connections.remove(pair)
            except ValueError:
                pass

    def disconnect(self):
        """Close all active connections. The proxy keeps listening for new ones."""
        with self._lock:
            pairs = list(self._connections)
            self._connections.clear()
        for client, upstream in pairs:
            for sock in (client, upstream):
                try:
                    sock.close()
                except OSError:
                    pass

    def close(self):
        """Shut down the proxy entirely."""
        self._running = False
        self.disconnect()
        self._server.close()
        self._thread.join(timeout=2)
