"""Test adapter shim: proxies the CLI adapter protocol over a Unix socket.

discover: prints the manifest from COFLUX_TEST_MANIFEST env var.
execute:  bidirectional proxy between stdin/stdout and a Unix socket.
"""

import os
import socket
import sys
import threading


def discover():
    manifest = os.environ.get("COFLUX_TEST_MANIFEST", "")
    if not manifest:
        print("COFLUX_TEST_MANIFEST not set", file=sys.stderr)
        sys.exit(1)
    print(manifest)


def execute():
    socket_path = os.environ.get("COFLUX_TEST_SOCKET", "")
    if not socket_path:
        print("COFLUX_TEST_SOCKET not set", file=sys.stderr)
        sys.exit(1)

    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.connect(socket_path)

    stdin_fd = sys.stdin.buffer.fileno()
    stdout_fd = sys.stdout.buffer.fileno()

    def stdin_to_socket():
        while data := os.read(stdin_fd, 4096):
            conn.sendall(data)
        # Stdin closed (Go pool called Wait/Close). Shut down writes to
        # tell the test socket we're done sending.
        try:
            conn.shutdown(socket.SHUT_WR)
        except OSError:
            pass

    def socket_to_stdout():
        try:
            while data := conn.recv(4096):
                os.write(stdout_fd, data)
        except OSError:
            pass

    t1 = threading.Thread(target=stdin_to_socket)
    t2 = threading.Thread(target=socket_to_stdout, daemon=True)
    t1.start()
    t2.start()

    # Wait for stdin to close (Go pool called Wait/Close), then exit.
    # t2 is a daemon thread — it will be cleaned up when the process exits.
    # By this point, any data from the test socket has already been relayed
    # to stdout (the Go pool reads the result before closing stdin).
    t1.join()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: adapter.py <discover|execute>", file=sys.stderr)
        sys.exit(1)

    match sys.argv[1]:
        case "discover":
            discover()
        case "execute":
            execute()
        case cmd:
            print(f"unknown command: {cmd}", file=sys.stderr)
            sys.exit(1)
