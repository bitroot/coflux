"""The prompt template, as the adapter hands it on."""

import coflux as cf


def test_a_template_loses_the_indentation_of_the_code_around_it():
    """Written as an indented triple-quoted string, a template carries the
    code's margin on every line, which Markdown would read as a code
    block. The margin comes off, and so do the blank lines the quotes
    leave at each end; indentation relative to the margin stays."""
    prompt = cf.Prompt(
        """
        Promote **{candidate}**?

        Trained with {{config}}

        - one
          - nested
        """,
        title="Promote",
    )
    assert prompt._template == (
        "Promote **{candidate}**?\n\nTrained with {{config}}\n\n- one\n  - nested"
    )


def test_a_plain_template_is_left_alone():
    assert cf.Prompt("Deploy {service}?")._template == "Deploy {service}?"


def test_copies_keep_the_dedented_template():
    prompt = cf.Prompt(
        """
        Approve {thing}?
        """
    )
    assert prompt.with_key("k")._template == "Approve {thing}?"
    assert prompt.with_actions("Yes", "No")._template == "Approve {thing}?"
