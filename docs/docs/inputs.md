# Inputs

Tasks and workflows request _input_ from a user mid-execution. This is useful for human-in-the-loop steps - approving a deployment, reviewing flagged content, supplying credentials that aren't safe to store, or any other point where the workflow needs information that isn't available up-front.

A prompt is presented in Studio (or fetched/answered from the CLI). The execution waits for a response, then resumes with the result.

## Defining a prompt

Prompts are defined with `cf.Prompt`. Submitting a prompt returns an `Input` handle (much like submitting a task returns an `Execution` handle):

```python
import coflux as cf

approve = cf.Prompt("Approve deployment of {service}?", title="Deployment")

@cf.workflow()
def deploy(service: str):
    approve(service=service)  # blocks until approved; raises InputDismissed if dismissed
    do_deploy(service)
```

A prompt with no `model` is an _approval_ prompt - it has no payload, just an approve/dismiss decision.

## Templates and placeholders

Prompt templates are rendered as Markdown, with placeholder keys substituted with the values passed as submission. Three placeholder forms control how each value is rendered:

| Form | Rendering |
|------|-----------|
| `{key}` | Inline - renders a formatted value inline. |
| `{{key}}` | Block - renders the value as a block-level element. |
| `{{{key}}}` | Markdown - renders the string as Markdown. |

```python
summarise = cf.Prompt(
    """
    ## Summarise {meeting}

    Transcript:

    {{transcript}}
    """,
    model=Summary,
)

summarise(meeting="Weekly sync", transcript=long_text)
```

Placeholder values are serialised when the prompt is submitted and resolved when it's displayed - so references to other executions, blobs, or assets remain live at render time, following the same behaviour as in log messages.

## Typed inputs (Pydantic models)

To collect structured data, pass a Pydantic model (or any class with `model_json_schema()` and `model_validate()`) as `model`:

```python
from typing import Literal
from pydantic import BaseModel, Field

class Label(BaseModel):
    sentiment: Literal["positive", "neutral", "negative"]
    topic: Literal["product", "shipping", "support", "other"]
    confidence: int = Field(ge=1, le=5)
    notes: str | None = None

label = cf.Prompt("Label this feedback:\n\n{text}", model=Label, title="Label")

@cf.workflow()
def annotate(sample_id: str, text: str):
    result = label(text=text)  # result is a Label instance
    store(sample_id, sentiment=result.sentiment, topic=result.topic)
```

The model's JSON schema is used to render a form in Studio, and to validate the response. Primitive types (`str`, `int`, `float`, `bool`, `date`, `time`, `datetime`) can also be specified as the `model`.

## Submitting without blocking

Calling the prompt (`prompt(...)`) blocks the execution until a response arrives. Use `prompt.submit(...)` to get an `Input` handle without blocking, then resolve it later - or combine it with [`cf.select`](./select.md) to wait alongside other handles:

```python
handle = label.submit(text="Shipping was slow but the product is great.")

# ...do other work...

result = handle.result()
```

To check whether a response has arrived without blocking, use `.poll()`. It returns the value if the input has resolved, or `default` (`None` by default) otherwise:

```python
handle = label.submit(text=text)

if (result := handle.poll()) is not None:
    process(result)
else:
    cf.log_info("Still waiting for a label")
```

`.poll(timeout=...)` will wait up to the given number of seconds for a response before returning `default`.

## Suspending while waiting

A response might take minutes, hours, or days to arrive - holding a worker slot open the whole time is wasteful. Resolving an input inside a [`cf.suspense`](./suspense.md) scope lets the execution suspend while it waits, and be resumed once the response is available:

```python
@cf.workflow()
def annotate(sample_id: str, text: str):
    handle = label.submit(text=text)
    with cf.suspense():
        result = handle.result()
    ...
```

Because the execution re-runs from the beginning on resumption, any work performed before the suspense block needs to be idempotent - typically by [memoising](./memoizing.md) the tasks it calls. Input memoisation (below) makes the prompt itself safe to re-submit.

## Memoisation

Inputs are automatically memoised within a run by their template and placeholder values, so a prompt that's submitted twice (e.g., after a re-run of a step) returns the same input handle and reuses the existing response.

If a placeholder varies between submissions - say a timestamp shown in the prompt - the auto-generated key will vary with it, and deduplication won't kick in. Set an explicit key with `with_key` to dedupe on a stable subset of the submission:

```python
from datetime import datetime

label.with_key(f"sample-{sample_id}").submit(
    text=text,
    requested_at=datetime.now().isoformat(),  # shown in the prompt; excluded from the key
)
```

## Routing

Prompts can be tagged with `requires` to control who can respond - for example, restricting an approval to a specific role or user. Tags are matched against the responding user's tags in Studio.

```python
deploy_prod = cf.Prompt(
    "Deploy {service} to production?",
    requires={"role": "release-manager"},
)
```

## Customising the prompt

| Option | Description |
|--------|-------------|
| `title` | A short title shown above the prompt. |
| `actions` | Custom labels for the respond/dismiss buttons as `(respond, dismiss)` (e.g. `("Approve", "Reject")`). |
| `schema` | A raw JSON schema (string or dict) - alternative to `model` when you don't have a Pydantic class. |
| `requires` | Routing tags. |

The fluent methods (`with_key`, `with_initial`, `with_actions`, `with_requires`) return a new `Prompt` with overrides applied, leaving the original unchanged. This makes it easy to define a prompt once and reuse it with per-submission tweaks:

```python
label.with_initial(Label(sentiment="positive", topic="product", confidence=4)).submit(text=text)
```

## Lifecycle

An input is in one of these states:

- **Pending** - submitted, awaiting a response.
- **Responded** - a value (or approval) was provided. `Input.result()` returns it.
- **Dismissed** - the responder explicitly dismissed the prompt. `Input.result()` raises `InputDismissed`.
- **Cancelled** - the input was cancelled programmatically (via `Input.cancel()` or `cf.cancel([...])`). Distinct from _dismissed_, which represents a deliberate user decision.

Inputs can be cancelled atomically alongside other handles using [`cf.cancel`](./python_reference.md#cancelhandles).

## Responding from the CLI

Inputs can also be listed and responded to from the CLI:

```bash
coflux inputs list                          # show pending inputs
coflux inputs inspect <input-id>            # show a prompt
coflux inputs respond <input-id> <value>    # submit a response (JSON)
coflux inputs dismiss <input-id>            # dismiss the prompt
```

This is useful for scripting or for systems where inputs are produced by other automation rather than interactive users.
