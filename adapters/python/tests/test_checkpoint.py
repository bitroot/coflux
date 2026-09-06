"""The reserved checkpoint namespace.

Names starting with ``_`` belong to the adapter — currently the cursors
behind stream suspension, which are named after the stream and view being
read. Reserving the whole prefix rather than individual names means later
internal state doesn't need another round of this.

The check lives in ``Checkpoint`` rather than in the context's
``checkpoint_get``/``checkpoint_set``/``checkpoint_reset``, because those
are the path the adapter's own cursors go through — validating there would
reject the very thing the prefix exists for.
"""

from __future__ import annotations

import pytest

from coflux.checkpoint import RESERVED_PREFIX, Checkpoint


def test_ordinary_names_are_accepted():
    assert Checkpoint("cursor").name == "cursor"
    assert Checkpoint("total", default=0.0).default == 0.0
    # Only the *leading* character is reserved.
    assert Checkpoint("last_seen").name == "last_seen"


@pytest.mark.parametrize("name", ["_cursor", "_", "_cursor/Erun:2_0/0,None,1"])
def test_reserved_names_are_rejected(name):
    with pytest.raises(ValueError, match="reserved"):
        Checkpoint(name)


def test_the_prefix_is_what_the_adapter_uses():
    """The cursor names the stream subscription generates have to be
    exactly what this rejects, or the two could drift apart."""
    from coflux.streams import _CURSOR_PREFIX

    assert _CURSOR_PREFIX.startswith(RESERVED_PREFIX)
