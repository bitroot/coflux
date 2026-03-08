"""Test adapter shim: proxies the CLI adapter protocol over a Unix socket.

discover: prints the manifest from the file specified by --manifest.
execute:  bidirectional proxy between stdin/stdout and a Unix socket (--socket).
"""

import argparse
import os
import socket
import sys
import threading


def discover(args):
    with open(args.manifest) as f:
        print(f.read(), end="")


def execute(args):
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.connect(args.socket)

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--socket")
    parser.add_argument("command", choices=["discover", "execute"])
    parser.add_argument("modules", nargs="*")
    args = parser.parse_args()

    if args.command == "discover":
        discover(args)
    elif args.socket:
        execute(args)
    else:
        print("--socket is required for execute", file=sys.stderr)
        sys.exit(1)
