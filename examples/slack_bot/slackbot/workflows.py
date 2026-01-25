import os
import random
import threading

import coflux as cf
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.web import WebClient


def _web_client():
    return WebClient(token=os.environ["SLACK_BOT_TOKEN"])


def _socket_client():
    return SocketModeClient(
        app_token=os.environ["SLACK_APP_TOKEN"],
        web_client=_web_client(),
    )


@cf.workflow()
def handle_event(event):
    if event["type"] != "message":
        cf.log_debug("Received unhandled event: {type}", type=event["type"])
        return
    cf.log_debug("Received message: {message}", message=event["text"])
    _web_client().reactions_add(
        name=random.choice(["one", "two", "three", "four", "five"]),
        channel=event["channel"],
        timestamp=event["ts"],
    )


def _handle_request(client: SocketModeClient, req: SocketModeRequest):
    print(req.to_dict())
    handle_event(req.payload["event"])
    client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))


@cf.workflow()
def slack_bot():
    client = _socket_client()
    client.socket_mode_request_listeners.append(_handle_request)
    cf.log_debug("Connecting...")
    client.connect()
    cf.log_info("Connected.")
    threading.Event().wait()
