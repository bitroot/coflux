# Examples → Slack bot

This example implements a basic chat bot. Using a 'sensor', it listens for messages from the Slack API (using the [socket mode](https://api.slack.com/apis/connections/socket) client). Each message that is sent to the bot will cause a workflow to be scheduled. The workflow simply adds a random Emoji reaction to the message.

## Slack bot setup

Setting up the Slack bot and configuring permissions etc is a bit involved, but there are instructions in the [Slack SDK documentation](https://slack.dev/python-slack-sdk/socket-mode/index.html).

## Running

Build Docker image:

```bash
docker build -t coflux_slackbot .
```

Run worker:

```bash
docker run --rm -t \
  --add-host host.docker.internal:host-gateway \
  -e COFLUX_HOST=host.docker.internal:7777 \
  -e COFLUX_PROJECT=... \
  -e SLACK_BOT_TOKEN=... \
  -e SLACK_APP_TOKEN=... \
  coflux_slackbot
```

Or with reload (allowing you to update the code without rebuilding/restarting):

```bash
docker run ... -v "$(pwd):/app" coflux_slackbot --reload
```
