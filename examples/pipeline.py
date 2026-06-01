"""A 3-stage pipeline wired with cooperative Queue: produce -> square -> sum.

Each stage runs concurrently; queues carry values and use a None sentinel to signal completion.

    uv run --no-sync python examples/pipeline.py
"""

import sys

import pyroutine as pr


def produce(out, count):
    for i in range(count):
        out.put(i)
    out.put(None)  # Sentinel indicating end of stream


def square(inp, out):
    while True:
        v = inp.get()
        if v is None:
            break
        out.put(v * v)
    out.put(None)  # Sentinel indicating end of stream


def main():
    assert not sys._is_gil_enabled(), "run me with python3.14t"

    count = 1000
    nums = pr.Queue(maxsize=64)
    squares = pr.Queue(maxsize=64)

    # Spawn pipeline stages concurrently
    pr.spawn(produce, nums, count)
    pr.spawn(square, nums, squares)

    # Consume on the main thread until sentinel is received
    total = 0
    while True:
        v = squares.get()
        if v is None:
            break
        total += v

    expected = sum(i * i for i in range(count))
    print(
        f"sum of squares 0..{count - 1} = {total} (expected {expected}, match={total == expected})"
    )


if __name__ == "__main__":
    main()
