from __future__ import annotations

from typing import Any

from aiokafka.helpers import create_ssl_context

from ..settings import PipelineSettings


def connection_kwargs(settings: PipelineSettings) -> dict[str, Any]:
    protocol = settings.KAFKA_SECURITY_PROTOCOL.upper()
    kwargs: dict[str, Any] = {
        "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "security_protocol": protocol,
    }
    if "SSL" in protocol:
        kwargs["ssl_context"] = create_ssl_context(cafile=settings.KAFKA_SSL_CAFILE or None)
    if protocol.startswith("SASL"):
        kwargs.update(
            sasl_mechanism=settings.KAFKA_SASL_MECHANISM or "PLAIN",
            sasl_plain_username=settings.KAFKA_SASL_USERNAME,
            sasl_plain_password=settings.KAFKA_SASL_PASSWORD,
        )
    return kwargs
