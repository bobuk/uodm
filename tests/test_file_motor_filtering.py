import pytest
from uodm.file_motor_filtering import get_field_value, compare_values, match_logical_operator, match_condition


def test_get_field_value():
    # Test basic field access
    doc = {"name": "test", "value": 42}
    assert get_field_value(doc, "name") == "test"
    assert get_field_value(doc, "value") == 42
    
    # Test nested field access
    nested_doc = {"user": {"profile": {"age": 25}}}
    assert get_field_value(nested_doc, "user.profile.age") == 25
    
    # Test non-existent fields
    assert get_field_value(doc, "missing") is None
    assert get_field_value(nested_doc, "user.missing") is None
    assert get_field_value(nested_doc, "user.profile.missing") is None


def test_compare_values():
    # Test basic comparison operators
    assert compare_values(10, "$gt", 5) is True
    assert compare_values(5, "$gt", 10) is False
    assert compare_values(5, "$lt", 10) is True
    assert compare_values(10, "$lt", 5) is False
    
    # Test equality operators
    assert compare_values("test", "$ne", "other") is True
    assert compare_values("test", "$ne", "test") is False
    
    # Test array operators
    assert compare_values("a", "$in", ["a", "b", "c"]) is True
    assert compare_values("d", "$in", ["a", "b", "c"]) is False
    assert compare_values("d", "$nin", ["a", "b", "c"]) is True
    
    # Test exists operator
    assert compare_values("value", "$exists", True) is True
    assert compare_values(None, "$exists", False) is True
    
    # Test regex operator
    assert compare_values("test123", "$regex", r"test\d+") is True
    assert compare_values("test", "$regex", r"\d+") is False


def test_match_logical_operator():
    doc = {"name": "test", "value": 42, "tags": ["a", "b"]}
    
    # Test $and operator
    and_conditions = [
        {"value": {"$gt": 30}},
        {"name": "test"}
    ]
    assert match_logical_operator(doc, "$and", and_conditions, match_condition) is True
    
    # Test $or operator
    or_conditions = [
        {"value": {"$lt": 30}},
        {"name": "test"}
    ]
    assert match_logical_operator(doc, "$or", or_conditions, match_condition) is True
    
    # Test empty conditions
    assert match_logical_operator(doc, "$and", [], match_condition) is True
    assert match_logical_operator(doc, "$or", [], match_condition) is False
    assert match_logical_operator(doc, "$nor", [], match_condition) is False


def test_match_condition():
    doc = {
        "name": "test",
        "value": 42,
        "nested": {"key": "value"},
        "tags": ["a", "b"]
    }
    
    # Test direct value matching
    assert match_condition(doc, "test", "name") is True
    assert match_condition(doc, "other", "name") is False
    
    # Test operator matching
    assert match_condition(doc, {"$gt": 30}, "value") is True
    assert match_condition(doc, {"$lt": 30}, "value") is False
    
    # Test nested field matching
    assert match_condition(doc, "value", "nested.key") is True
    
    # Test logical operator matching
    complex_condition = {
        "$and": [
            {"value": {"$gt": 30}},
            {"name": "test"}
        ]
    }
    assert match_condition(doc, complex_condition) is True
    
    # Test array conditions
    assert match_condition(doc, {"$in": ["a"]}, "tags") is True  # Test single element membership
    assert match_condition(doc, {"$in": ["c"]}, "tags") is False  # Test non-membership
    
    # Test non-existent fields
    assert match_condition(doc, {"$exists": False}, "missing") is True
    assert match_condition(doc, {"$exists": True}, "name") is True


def test_integration_complex_queries():
    doc = {
        "name": "test_doc",
        "value": 42,
        "nested": {
            "level1": {
                "level2": "deep_value"
            }
        },
        "tags": ["a", "b", "c"],
        "status": "active"
    }
    
    # Complex nested query
    complex_query = {
        "$and": [
            {"value": {"$gt": 40}},
            {"nested.level1.level2": "deep_value"},
            {"$or": [
                {"tags": {"$in": ["x", "y", "a"]}},
                {"status": "active"}
            ]}
        ]
    }
    
    assert match_condition(doc, complex_query) is True
    
    # Modify query to fail
    failing_query = {
        "$and": [
            {"value": {"$gt": 50}},  # This will fail
            {"nested.level1.level2": "deep_value"},
            {"$or": [
                {"tags": {"$in": ["x", "y", "a"]}},
                {"status": "active"}
            ]}
        ]
    }
    
    assert match_condition(doc, failing_query) is False
