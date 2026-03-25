"""
Stripe Service for MiniMe Platform
Handles subscription management, payments, and webhooks
Supports both test and production modes via environment variables
"""

import stripe
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal

# Initialize Stripe with secret key from environment
stripe.api_key = os.getenv('STRIPE_SECRET_KEY')

# Pricing configuration (use environment variables for Price IDs)
PRICING_PLANS = {
    'free': {
        'name': 'Free',
        'price': 0,
        'stripe_price_id': None,  # No Stripe price for free tier
        'limits': {
            'activities_per_month': 100,
            'graph_nodes': 100,
            'api_calls_per_day': 50,
        },
        'features': [
            'Basic analytics',
            '100 activities/month',
            '7-day weekly digests',
            'Community support',
        ]
    },
    'pro': {
        'name': 'Pro',
        'price': 19,  # $19/month
        'stripe_price_id': os.getenv('STRIPE_PRICE_PRO_MONTHLY', 'price_pro_test'),
        'limits': {
            'activities_per_month': -1,  # Unlimited
            'graph_nodes': 500,
            'api_calls_per_day': 1000,
        },
        'features': [
            'Unlimited activities',
            'Advanced analytics',
            'Real-time insights',
            'Skills tracking',
            'Knowledge graph (500 nodes)',
            'Email support',
        ]
    },
    'enterprise': {
        'name': 'Enterprise',
        'price': 99,  # $99/month
        'stripe_price_id': os.getenv('STRIPE_PRICE_ENTERPRISE_MONTHLY', 'price_enterprise_test'),
        'limits': {
            'activities_per_month': -1,  # Unlimited
            'graph_nodes': -1,  # Unlimited
            'api_calls_per_day': -1,  # Unlimited
        },
        'features': [
            'Everything in Pro',
            'Unlimited knowledge graph',
            'Custom integrations',
            'API access',
            'Priority support',
            'Team features',
        ]
    }
}


class StripeService:
    """Service class for Stripe operations"""
    
    @staticmethod
    def create_customer(email: str, user_id: int, metadata: Optional[Dict] = None) -> stripe.Customer:
        """Create a Stripe customer"""
        customer_metadata = metadata or {}
        customer_metadata['user_id'] = str(user_id)
        
        customer = stripe.Customer.create(
            email=email,
            metadata=customer_metadata
        )
        return customer
    
    @staticmethod
    def create_checkout_session(
        customer_id: str,
        price_id: str,
        success_url: str,
        cancel_url: str,
        metadata: Optional[Dict] = None
    ) -> stripe.checkout.Session:
        """Create a Stripe Checkout session"""
        session = stripe.checkout.Session.create(
            customer=customer_id,
            payment_method_types=['card'],
            line_items=[{
                'price': price_id,
                'quantity': 1,
            }],
            mode='subscription',
            success_url=success_url,
            cancel_url=cancel_url,
            metadata=metadata or {},
            allow_promotion_codes=True,
            billing_address_collection='auto',
        )
        return session
    
    @staticmethod
    def get_subscription(subscription_id: str) -> Optional[stripe.Subscription]:
        """Retrieve a subscription"""
        try:
            return stripe.Subscription.retrieve(subscription_id)
        except stripe.error.InvalidRequestError:
            return None
    
    @staticmethod
    def cancel_subscription(
        subscription_id: str,
        at_period_end: bool = True
    ) -> stripe.Subscription:
        """Cancel a subscription"""
        if at_period_end:
            # Cancel at end of billing period
            subscription = stripe.Subscription.modify(
                subscription_id,
                cancel_at_period_end=True
            )
        else:
            # Cancel immediately
            subscription = stripe.Subscription.cancel(subscription_id)
        
        return subscription
    
    @staticmethod
    def update_subscription(
        subscription_id: str,
        new_price_id: str
    ) -> stripe.Subscription:
        """Update subscription to a new plan"""
        subscription = stripe.Subscription.retrieve(subscription_id)
        
        subscription = stripe.Subscription.modify(
            subscription_id,
            items=[{
                'id': subscription['items']['data'][0].id,
                'price': new_price_id,
            }],
            proration_behavior='create_prorations',
        )
        return subscription
    
    @staticmethod
    def get_invoices(customer_id: str, limit: int = 10) -> List[stripe.Invoice]:
        """Get customer invoices"""
        invoices = stripe.Invoice.list(
            customer=customer_id,
            limit=limit
        )
        return invoices.data
    
    @staticmethod
    def create_customer_portal_session(
        customer_id: str,
        return_url: str
    ) -> stripe.billing_portal.Session:
        """Create a customer portal session for self-service"""
        session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=return_url
        )
        return session
    
    @staticmethod
    def construct_webhook_event(payload: bytes, sig_header: str, webhook_secret: str):
        """Verify and construct webhook event"""
        try:
            event = stripe.Webhook.construct_event(
                payload, sig_header, webhook_secret
            )
            return event
        except ValueError:
            # Invalid payload
            raise ValueError("Invalid payload")
        except stripe.error.SignatureVerificationError:
            # Invalid signature
            raise stripe.error.SignatureVerificationError("Invalid signature")
    
    @staticmethod
    def get_plan_limits(plan_type: str) -> Dict[str, Any]:
        """Get usage limits for a plan"""
        return PRICING_PLANS.get(plan_type, PRICING_PLANS['free'])['limits']
    
    @staticmethod
    def check_usage_limit(
        current_usage: int,
        limit: int,
        buffer_percent: float = 0.1
    ) -> Dict[str, Any]:
        """Check if usage is approaching or exceeding limit"""
        if limit == -1:  # Unlimited
            return {
                'exceeded': False,
                'warning': False,
                'percent_used': 0,
            }
        
        percent_used = (current_usage / limit) * 100 if limit > 0 else 0
        warning_threshold = (1 - buffer_percent) * 100  # 90% by default
        
        return {
            'exceeded': current_usage >= limit,
            'warning': percent_used >= warning_threshold,
            'percent_used': round(percent_used, 2),
            'current': current_usage,
            'limit': limit,
            'remaining': max(0, limit - current_usage)
        }
    
    @staticmethod
    def format_price(amount_cents: int, currency: str = 'usd') -> str:
        """Format price for display"""
        amount = amount_cents / 100
        if currency.lower() == 'usd':
            return f"${amount:.2f}"
        return f"{amount:.2f} {currency.upper()}"
    
    @staticmethod
    def get_all_plans() -> Dict[str, Dict]:
        """Get all available pricing plans"""
        return PRICING_PLANS


# Utility functions
def is_test_mode() -> bool:
    """Check if Stripe is in test mode"""
    api_key = stripe.api_key or ""
    return api_key.startswith('sk_test_')


def get_publishable_key() -> str:
    """Get the appropriate publishable key"""
    return os.getenv('STRIPE_PUBLISHABLE_KEY', '')
