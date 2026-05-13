from collections.abc import Iterator
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from io import StringIO


@contextmanager
def suppress_external_output() -> Iterator[None]:
    """Suppress noisy stdout/stderr output from third-party libraries."""
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        yield
