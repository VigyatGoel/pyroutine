"""Simple Python-style concurrent HTTP server example using pyroutine.

Starts a server on port 8080.
- GET http://127.0.0.1:8080/ returns "Hello, World!"
- Any other path returns a "404 Not Found"
"""

import pyroutine as pr

# Create the App router
app = pr.http.App()


@app.get("/")
def hello(request):
    """Clean Python-style request handler."""
    return "Hello, World!"


@app.route("/unknown", methods=["GET"])
def fallback(request):
    """Fallback handler."""
    return "404 Not Found", 404


def main():
    address = "127.0.0.1:8080"
    print(f"Starting concurrent Pythonic web server on http://{address}/")
    print("Press Ctrl+C to stop.")
    print("To test: run 'curl http://127.0.0.1:8080/' in another terminal.")

    # Run the server directly on the main thread
    app.run(address)


if __name__ == "__main__":
    main()
