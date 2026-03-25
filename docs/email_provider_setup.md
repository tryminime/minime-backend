# Email Provider Integration Guide

This guide covers integrating an email provider for daily/weekly analytics emails.

## Recommended: SendGrid

**Why SendGrid**:
- Simple REST API
- Free tier: 100 emails/day
- Excellent deliverability
- Detailed analytics (opens, clicks, bounces)
- Template management
- Easy Python integration

### Setup

1. **Sign up**: https://signup.sendgrid.com/
2. **Create API Key**: Settings → API Keys → Create API Key
3. **Add to environment**:
   ```bash
   export SENDGRID_API_KEY='SG.xxxxxxxxxxxxx'
   ```

4. **Install package**:
   ```bash
   pip install sendgrid
   ```

5. **Update settings.py**:
   ```python
   SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
   DEFAULT_FROM_EMAIL = 'MiniMe Analytics <analytics@minime.app>'
   ```

### Implementation

Update `backend/tasks/analytics_tasks.py`:

```python
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

def send_email_via_sendgrid(to_email, subject, html_content):
    """Send email via SendGrid."""
    message = Mail(
        from_email=Email(settings.DEFAULT_FROM_EMAIL),
        to_emails=To(to_email),
        subject=subject,
        html_content=Content("text/html", html_content)
    )
    
    try:
        sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
        response = sg.send(message)
        
        return {
            'status': 'sent',
            'provider_message_id': response.headers.get('X-Message-Id'),
            'status_code': response.status_code
        }
    except Exception as e:
        logger.error(f"SendGrid error: {e}")
        return {'status': 'failed', 'error': str(e)}
```

Replace in `send_daily_summary_email_task`:

```python
# TODO: Send actual email via SendGrid/SES
# For now, just log
logger.info("Email would be sent here", ...)

# REPLACE WITH:
result = send_email_via_sendgrid(
    to_email=user.email,
    subject=f"Your Daily Summary - {target_date.strftime('%b %d, %Y')}",
    html_content=email_html
)

email_record.status = result['status']
email_record.provider_message_id = result.get('provider_message_id')
```

### Tracking Opens/Clicks

SendGrid automatically tracks:
- **Opens**: Via tracking pixel
- **Clicks**: Via URL rewriting
- **Bounces**: Via webhooks
- **Spam reports**: Via webhooks

Enable in SendGrid Settings → Tracking.

### Webhooks (Optional)

Update `analytics_emails` table via webhooks:

```python
@app.post("/api/v1/webhooks/sendgrid")
async def sendgrid_webhook(request: Request):
    events = await request.json()
    
    for event in events:
        if event['event'] == 'open':
            # Update opened_at
            pass
        elif event['event'] == 'click':
            # Update clicked_at
            pass
    
    return {"status": "ok"}
```

---

## Alternative: AWS SES

**Why AWS SES**:
- Extremely cheap ($0.10/1000 emails)
- High volume capability
- Integrated with AWS ecosystem

**Setup**:
1. AWS Console → SES → Verify domain
2. Create IAM user with SES permissions
3. Install: `pip install boto3`

**Code**:
```python
import boto3

ses = boto3.client('ses', region_name='us-east-1')

ses.send_email(
    Source=settings.DEFAULT_FROM_EMAIL,
    Destination={'ToAddresses': [user.email]},
    Message={
        'Subject': {'Data': subject},
        'Body': {'Html': {'Data': html_content}}
    }
)
```

---

## Alternative: Mailgun

**Why Mailgun**:
- Developer-friendly API
- Good deliverability
- Free tier: 5,000 emails/month

**Setup**:
1. Sign up: https://mailgun.com
2. Verify domain
3. Install: `pip install mailgun2`

---

## Recommendation

**Use SendGrid** for:
- Simple setup
- Analytics dashboard
- Template management
- Good free tier

**Use AWS SES** if:
- Already on AWS
- High volume (>10k emails/month)
- Cost is critical

---

## Testing Email Rendering

### Tools
1. **Litmus**: https://litmus.com (paid)
2. **Email on Acid**: https://www.emailonacid.com (paid)
3. **Mailtrap**: https://mailtrap.io (free tier)

### Manual Testing
1. Gmail (desktop + mobile)
2. Outlook (desktop)
3. Apple Mail (macOS + iOS)
4. Yahoo Mail

### Key Checks
- ✅ Metrics cards display correctly
- ✅ Inline CSS renders
- ✅ CTA button clickable
- ✅ Images load (if any)
- ✅ Links work
- ✅ Unsubscribe link present

---

## Production Checklist

- [ ] Email provider configured (SendGrid/SES)
- [ ] API key in environment variables
- [ ] Default FROM email set
- [ ] Unsubscribe link implemented
- [ ] Email templates tested in 3+ clients
- [ ] Tracking enabled (opens/clicks)
- [ ] Webhooks configured (optional)
- [ ] Rate limiting configured
- [ ] Error notifications setup
- [ ] Bounce handling implemented

---

## Next Steps

1. Choose provider (recommend SendGrid)
2. Sign up and get API key
3. Update `analytics_tasks.py` with provider code
4. Test email delivery
5. Verify tracking works
6. Test across email clients
7. Deploy to production
