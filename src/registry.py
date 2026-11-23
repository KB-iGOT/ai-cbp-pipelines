"""Pipeline name -> class. Adding a pipeline = new folder + one line here."""
from __future__ import annotations

from .common.base_pipeline import BasePipeline
from .course_ingestion.pipeline import CourseIngestionPipeline
from .course_retirement.pipeline import CourseRetirementPipeline

PIPELINES: dict[str, type[BasePipeline]] = {
    cls.name: cls
    for cls in (
        CourseIngestionPipeline,
        CourseRetirementPipeline,
    )
}


def get_pipeline(name: str) -> type[BasePipeline]:
    try:
        return PIPELINES[name]
    except KeyError:
        raise SystemExit(f"Unknown pipeline {name!r}. Available: {', '.join(sorted(PIPELINES))}") from None
