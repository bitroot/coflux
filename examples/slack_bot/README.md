# Examples → Slack bot

This example implements a basic chat bot. Using a sensor, it listens for messages from the Slack API (using the [socket mode](https://api.slack.com/apis/connections/socket) client). Each message that is sent to the bot will cause a workflow run to be scheduled. The workflow adds a random Emoji reaction to the message.

## Slack bot setup

Setting up the Slack bot and configuring permissions etc is a bit involved, but there are instructions in the [Slack SDK documentation](https://slack.dev/python-slack-sdk/socket-mode/index.html).

# Running

Requires `uv`.

Install dependencies:

```bash
uv sync
```

Configure Coflux:

```bash
uv run configure --project=... --space=...
```

Run worker in development mode:

```bash
uv run coflux worker --dev slackbot.workflows
```

In your Slack workspace, send a message to the app. This should launch a workflow run which will add an emoji reaction to the message.
