"""Guard: pyroutine is meant for the free-threaded build. Fail loudly otherwise."""

import sys


def test_gil_is_disabled():
    assert hasattr(sys, "_is_gil_enabled"), "not a 3.13+ build"
    assert sys._is_gil_enabled() is False, (
        "pyroutine requires a free-threaded interpreter (python3.14t); "
        "the GIL is currently enabled"
    )
