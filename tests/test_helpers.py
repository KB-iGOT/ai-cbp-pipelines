from collections import defaultdict

from src.course_ingestion.utils import (
    build_combined_text,
    build_description_text,
    build_keywords_text,
    is_course,
    is_older,
    normalise_level,
)
from src.course_retirement.references import remove_from_list, remove_from_plan_data


# ── course_ingestion/utils.py ────────────────────────────────────────────────
def test_is_course():
    assert is_course({"primaryCategory": "Course", "courseCategory": "Course"})
    assert is_course({"primaryCategory": "Course", "courseCategory": ["Course"]})
    assert not is_course({"primaryCategory": "Course", "courseCategory": "Program"})


def test_is_older():
    assert is_older("100", "200")
    assert not is_older("300", "200")
    assert not is_older("200", None)
    assert not is_older(None, "200")
    assert not is_older("abc", "200")


def test_normalise_level():
    assert normalise_level(" Beginner ") == "Beginner"
    assert normalise_level("NA") is None
    assert normalise_level("") is None
    assert normalise_level(None) is None


def test_text_builders():
    course = {
        "name": " Data Ethics ",
        "description": "About data",
        "keywords": ["data", "ethics"],
        "instructions": "<p>Watch   <b>all</b> videos</p>",
        "source": "iGOT",
        "difficultyLevel": "Beginner",
        "courseLevel": "Intermediate",
        "competencies_v6": [{"competencyAreaName": "Functional", "competencyThemeName": "Data",
                             "competencySubThemeName": "Ethics"}],
    }
    assert build_description_text(course) == "title: Data Ethics | text: Description: About data"
    assert build_keywords_text(course) == "title: Data Ethics | text: Keywords: data, ethics"
    assert build_combined_text(course) == (
        "title: Data Ethics | text: Name: Data Ethics | Instructions: Watch all videos | Source: iGOT | "
        "Difficulty Level: Beginner | Course Level: Intermediate | "
        "Competencies: [1] Type: Functional -> Theme: Data -> Sub-Theme: Ethics"
    )


def test_combined_text_with_missing_course_level():
    assert "Course Level:  |" in build_combined_text({"name": "X", "courseLevel": "NA"})


# ── course_retirement/references.py ──────────────────────────────────────────
def test_remove_from_list_keeps_order_and_counts_hits():
    hits = defaultdict(int)
    values = [{"identifier": "a"}, {"identifier": "do_x"}, {"identifier": "b"}, {"identifier": "do_x"}]
    result, changed = remove_from_list(values, {"do_x"}, hits)
    assert changed
    assert result == [{"identifier": "a"}, {"identifier": "b"}]
    assert hits == {"do_x": 2}


def test_remove_from_list_bare_ids_and_non_list():
    hits = defaultdict(int)
    assert remove_from_list(["a", "do_x"], {"do_x"}, hits, bare_ids=True) == (["a"], True)
    assert remove_from_list(None, {"do_x"}, hits) == (None, False)
    assert remove_from_list([{"identifier": "a"}], {"do_x"}, hits) == ([{"identifier": "a"}], False)


def test_remove_from_plan_data_list_and_object():
    hits = defaultdict(int)
    plans = [{"name": "p1", "selected_courses": [{"identifier": "do_x"}, {"identifier": "a"}]},
             {"name": "p2", "selected_courses": [{"identifier": "b"}]}]
    result, changed = remove_from_plan_data(plans, {"do_x"}, hits)
    assert changed
    assert result[0]["selected_courses"] == [{"identifier": "a"}]
    assert result[1] is plans[1]

    single, changed = remove_from_plan_data({"selected_courses": [{"identifier": "do_x"}]}, {"do_x"}, hits)
    assert changed and single == {"selected_courses": []}
