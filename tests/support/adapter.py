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

    # Exit when EITHER direction ends:
    #   * stdin closes — the CLI's old pattern (break loop, Wait, close stdin).
    #   * the test's socket closes — the new pattern, simulating the real
    #     adapter exiting after its task finishes. The CLI's post-fix receive
    #     loop stays open until EOF on adapter stdout, so mock-side socket
    #     close is how tests signal "this execution's work is done".
    done = threading.Event()

    def stdin_to_socket():
        try:
            while data := os.read(stdin_fd, 4096):
                conn.sendall(data)
        except OSError:
            pass
        try:
            conn.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        done.set()

    def socket_to_stdout():
        try:
            while data := conn.recv(4096):
                os.write(stdout_fd, data)
        except OSError:
            pass
        # Test closed the socket. Close stdout so CLI's readLoop gets EOF,
        # breaks its receive loop, and runs Wait (which will close stdin).
        try:
            os.close(stdout_fd)
        except OSError:
            pass
        done.set()

    threading.Thread(target=stdin_to_socket, daemon=True).start()
    threading.Thread(target=socket_to_stdout, daemon=True).start()

    done.wait()


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
