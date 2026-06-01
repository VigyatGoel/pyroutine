"""Example of using pyroutine.http to make cooperative HTTP and HTTPS requests.

Demonstrates:
1. Simple HTTPS GET request
2. Simple HTTPS POST request (transmitting data)
3. Concurrent GET requests fanning out in parallel using spawn and gather
"""

import json
import time
import pyroutine as pr


# 1. Simple GET request
def simple_get():
    print("--- 1. Simple HTTPS GET ---")
    try:
        # Fetch data cooperatively over HTTPS
        res = pr.http.get("https://httpbin.org/get")
        print(f"Status: {res.status_code}")
        print("Response headers (keys):", list(res.headers.keys()))
        print("Response Text:")
        print(res.text[:300] + "...\n")
    except Exception as e:
        print(f"GET failed: {e}\n")


# 2. Simple POST request
def simple_post():
    print("--- 2. Simple HTTPS POST ---")
    try:
        post_data = "hello from pyroutine client!"
        res = pr.http.post("https://httpbin.org/post", data=post_data)
        print(f"Status: {res.status_code}")

        # Parse the JSON response returned by httpbin
        parsed = json.loads(res.text)
        print(f"httpbin echoed data: {parsed.get('data')!r}\n")
    except Exception as e:
        print(f"POST failed: {e}\n")


# 3. Concurrent Fan-out requests
def fetch_url(url):
    try:
        print(f"Starting fetch: {url}")
        res = pr.http.get(url)
        print(f"Finished fetch: {url} -> Status {res.status_code}")
        return res.status_code
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return -1


def concurrent_fanout():
    print("--- 3. Concurrent HTTPS GET Fan-out ---")
    urls = [
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/delay/1",
    ]

    t0 = time.monotonic()

    # Spawn tasks concurrently
    tasks = [pr.spawn(fetch_url, url) for url in urls]

    # Gather results cooperatively
    statuses = pr.gather(*tasks)

    print(f"Statuses: {statuses}")
    print(
        f"All fetches completed in {time.monotonic() - t0:.3f}s! "
        f"(Since they ran concurrently, total time is governed by the slowest 2s call)\n"
    )


def main():
    # Run the tasks concurrently using structured spawn/gather
    pr.spawn(simple_get).join()
    pr.spawn(simple_post).join()
    pr.spawn(concurrent_fanout).join()


if __name__ == "__main__":
    main()
