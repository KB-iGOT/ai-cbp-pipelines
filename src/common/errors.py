"""Exceptions that tell the worker what to do with a failed event."""


class PipelineError(Exception):
    pass


class SkipEvent(PipelineError):
    """The event is intentionally ignored. Not retried, not dead-lettered."""


class NonRetryableError(PipelineError):
    """Retrying will not help (bad payload, 4xx from the API). Sent straight to the DLQ."""


class InvalidEventError(NonRetryableError):
    """The message failed validation (wrong shape, missing or malformed field, unexpected
    operationType). Not retried; sent straight to the DLQ with x-error-type=InvalidEventError,
    so it is never silently dropped."""


class StaleReadError(PipelineError):
    """The search index has not caught up with the event yet. Retried with backoff."""


class EmbeddingError(RuntimeError):
    """Gemini returned no embedding. Treated like any other error: retried with backoff, then DLQ."""


class FatalPipelineError(PipelineError):
    """Stop the worker without committing offsets (e.g. the KB token was rejected).

    The pod restarts and re-reads the same messages, instead of dead-lettering everything.
    """
