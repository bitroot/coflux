ExUnit.start()

# The server secret is global state (`:persistent_term`, set from the
# environment at startup), and signing a service token or encrypting a
# secret reads it. The application isn't started under test, so set one
# for the whole run - a test that needs it to be a particular value, or
# absent, has to own the global while nothing else is running, which
# means `async: false`.
:persistent_term.put(:coflux_secret, "test-secret")
