"""Kafka-driven pipelines that keep the course embeddings table in sync with the platform.

Run one pipeline per process:

    python -m src.runner course_ingestion
    python -m src.runner course_retirement
"""
