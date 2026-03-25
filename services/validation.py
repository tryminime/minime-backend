"""
Validation utilities for Personal Analytics services.

Provides input validation for:
- UUID format validation
- Date range validation
- Type checking helpers
"""

from datetime import date, datetime, timedelta
from typing import Optional
import uuid as uuid_lib


class ValidationError(Exception):
    """Raised when input validation fails."""
    pass


def validate_uuid(user_id: str, param_name: str = "user_id") -> None:
    """
    Validate UUID format.
    
    Args:
        user_id: String to validate as UUID
        param_name: Parameter name for error messages
        
    Raises:
        ValidationError: If not a valid UUID
    """
    if not user_id:
        raise ValidationError(f"{param_name} is required")
    
    try:
        uuid_lib.UUID(user_id)
    except (ValueError, AttributeError, TypeError) as e:
        raise ValidationError(f"{param_name} must be a valid UUID, got: {user_id}") from e


def validate_date(target_date: date, param_name: str = "date", allow_future: bool = False) -> None:
    """
    Validate date is reasonable.
    
    Args:
        target_date: Date to validate
        param_name: Parameter name for error messages
        allow_future: Whether to allow future dates
        
    Raises:
        ValidationError: If date is invalid
    """
    if not isinstance(target_date, date):
        raise ValidationError(f"{param_name} must be a date object, got: {type(target_date)}")
    
    # Check not too far in past (10 years)
    ten_years_ago = date.today() - timedelta(days=365 * 10)
    if target_date < ten_years_ago:
        raise ValidationError(f"{param_name} cannot be more than 10 years in the past")
    
    # Check not in future
    if not allow_future and target_date > date.today():
        raise ValidationError(f"{param_name} cannot be in the future")


def validate_date_range(
    start_date: date,
    end_date: date,
    max_days: Optional[int] = None
) -> None:
    """
    Validate date range is reasonable.
    
    Args:
        start_date: Start of range
        end_date: End of range
        max_days: Maximum allowed range in days
        
    Raises:
        ValidationError: If range is invalid
    """
    validate_date(start_date, "start_date", allow_future=True)
    validate_date(end_date, "end_date", allow_future=True)
    
    if end_date < start_date:
        raise ValidationError("end_date must be >= start_date")
    
    if max_days:
        range_days = (end_date - start_date).days
        if range_days > max_days:
            raise ValidationError(
                f"Date range too large: {range_days} days (max: {max_days})"
            )


def validate_week_start(week_start: date) -> date:
    """
    Validate and adjust week_start to Monday.
    
    Args:
        week_start: Date to validate/adjust
        
    Returns:
        Monday of the week containing week_start
        
    Raises:
        ValidationError: If date is invalid
    """
    validate_date(week_start, "week_start", allow_future=True)
    
    # Adjust to Monday
    if week_start.weekday() != 0:
        adjusted = week_start - timedelta(days=week_start.weekday())
        return adjusted
    
    return week_start


def validate_score_range(score: float, min_val: float, max_val: float, param_name: str) -> None:
    """
    Validate numeric score is in range.
    
    Args:
        score: Score to validate
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        param_name: Parameter name for error messages
        
    Raises:
        ValidationError: If out of range
    """
    if not isinstance(score, (int, float)):
        raise ValidationError(f"{param_name} must be numeric, got: {type(score)}")
    
    if not (min_val <= score <= max_val):
        raise ValidationError(
            f"{param_name} must be between {min_val} and {max_val}, got: {score}"
        )
