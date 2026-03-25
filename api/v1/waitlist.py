"""
Waitlist API endpoints for beta user signup
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/waitlist", tags=["Waitlist"])


class WaitlistSignup(BaseModel):
    """Waitlist signup request"""
    firstName: str
    lastName: str
    email: EmailStr
    company: Optional[str] = None
    role: Optional[str] = None
    useCase: Optional[str] = None


class WaitlistResponse(BaseModel):
    """Waitlist signup response"""
    success: bool
    message: str
    position: Optional[int] = None


@router.post("", response_model=WaitlistResponse)
async def join_waitlist(signup: WaitlistSignup):
    """
    Add user to beta waitlist
    
    Args:
        signup: Waitlist signup form data
        
    Returns:
        Confirmation with waitlist position
    """
    try:
        # TODO: Save to database
        # For now, just log it
        logger.info(f"New waitlist signup: {signup.email} ({signup.firstName} {signup.lastName})")
        
        # TODO: Send confirmation email using existing email service
        
        # Mock position (in production, query database)
        position = 732
        
        return WaitlistResponse(
            success=True,
            message=f"Welcome to the waitlist, {signup.firstName}! We'll send you an invite code soon.",
            position=position
        )
        
    except Exception as e:
        logger.error(f"Error adding to waitlist: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to join waitlist. Please try again later."
        )


@router.get("/stats")
async def get_waitlist_stats():
    """
    Get public waitlist statistics
    
    Returns:
        Current waitlist stats (total signups, spots remaining)
    """
    # TODO: Query database for actual stats
    return {
        "total_signups": 732,
        "total_spots": 1000,
        "spots_remaining": 268,
        "percentage_filled": 73.2
    }
