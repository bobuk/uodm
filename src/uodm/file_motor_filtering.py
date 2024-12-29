import re
from typing import Any


def get_field_value(document: dict, field_path: str) -> Any:
    """Extract nested field value from document."""
    value = document
    for part in field_path.split("."):
        if isinstance(value, dict):
            value = value.get(part)
            if value is None:
                return None
        else:
            return None
    return value


def compare_values(value: Any, operator: str, target_value: Any) -> bool:
    """Compare values using MongoDB-style operators."""
    if operator == "$exists":
        result = (value is not None) == target_value
        return result

    # Return False if value is None for all other operators
    if value is None:
        return False

    # Handle numeric comparisons
    if operator in ["$gt", "$lt", "$gte", "$lte"]:
        try:
            # Ensure both values are the same type for comparison
            if isinstance(value, int) and isinstance(target_value, int):
                num_value = value
                num_target = target_value
            else:
                num_value = float(value)
                num_target = float(target_value)

            if operator == "$gt":
                result = num_value > num_target
                return result
            elif operator == "$lt":
                result = num_value < num_target
                return result
            elif operator == "$gte":
                result = num_value >= num_target
                return result
            elif operator == "$lte":
                result = num_value <= num_target
                return result
        except (ValueError, TypeError) as e:
            print(f"Error in numeric comparison: {e}")
            return False

    # Handle other operators
    elif operator == "$ne":
        return value != target_value
    elif operator == "$in":
        if isinstance(value, list):
            # If value is a list, check if any element from target_value is in it
            return any(t in value for t in target_value)
        else:
            # If value is not a list, check if it's in target_value
            return value in target_value
    elif operator == "$nin":
        return value not in target_value
    elif operator == "$regex":
        return bool(re.search(target_value, str(value)))

    # Return False for unknown operators
    return False


def match_logical_operator(document: dict, operator: str, conditions: Any, match_condition_func: Any) -> bool:
    """Handle logical operators ($and, $or, $nor)."""
    if not isinstance(conditions, list):
        raise TypeError(f"Conditions for {operator} must be a list, got {type(conditions)}")
    
    if not conditions:  # Handle empty conditions list
        if operator == "$and":
            return True  # Empty $and is always true
        elif operator in ("$or", "$nor"):
            return False  # Empty $or/$nor is always false
    
    if operator == "$and":
        return all(match_condition_func(document, cond) for cond in conditions)
    elif operator == "$or":
        return any(match_condition_func(document, cond) for cond in conditions)
    elif operator == "$not":
        if len(conditions) != 1:
            raise ValueError("$not operator requires exactly one condition")
        return not match_condition_func(document, conditions[0])
    
    raise ValueError(f"Unknown logical operator: {operator}")


def match_condition(document: dict, condition: Any, key: str | None = None) -> bool:
    """Match a document against a query condition."""
    # Handle direct value comparison
    if not isinstance(condition, dict):
        value = get_field_value(document, key) if key else document
        return value == condition

    # Handle logical operators first
    for op in ["$and", "$or", "$nor"]:
        if op in condition:
            return match_logical_operator(document, op, condition[op], match_condition)
    if "$not" in condition:
        return not match_condition(document, condition["$not"])

    # Handle field conditions
    if key is not None:
        value = get_field_value(document, key)
        if value is None:
            # Special case for $exists operator
            if len(condition) == 1 and "$exists" in condition:
                return not condition["$exists"]
            return False
        
        # Handle comparison operators
        for op, val in condition.items():
            if not compare_values(value, op, val):
                return False
        return True
    else:
        # For nested conditions, each condition in the list needs to be evaluated independently
        if isinstance(condition, list):
            return all(match_condition(document, cond) for cond in condition)
        
        # Handle regular nested conditions
        for field_key, field_condition in condition.items():
            if field_key.startswith("$"):
                # This is a logical operator
                return match_logical_operator(document, field_key, [field_condition], match_condition)
            if not match_condition(document, field_condition, field_key):
                return False
        return True
