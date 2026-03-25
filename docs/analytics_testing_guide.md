# Analytics Integration Testing Guide

End-to-end testing for daily analytics pipeline.

## Overview

Tests the complete flow:
1. Compute daily metrics
2. Generate LLM summary
3. Render email template
4. Send email (mock or real)

---

## Prerequisites

### 1. Start Services

```bash
# Terminal 1: PostgreSQL + Redis
docker start minime-postgres minime-redis

# Terminal 2: Celery worker
cd backend
source venv/bin/activate
celery -A celery_app worker --loglevel=info -Q analytics

# Terminal 3: Celery beat
celery -A celery_app beat --loglevel=info
```

### 2. Environment Variables

```bash
# .env file
ANTHROPIC_API_KEY=sk-ant-xxxxx
SENDGRID_API_KEY=SG.xxxxx  # Optional
REDIS_URL=redis://localhost:6379/0
DATABASE_URL=postgresql://minime:password@localhost:5432/minime_db
```

---

## Test 1: Manual Metrics Computation

```python
from datetime import date
from backend.services.productivity_metrics_service import ProductivityMetricsService
from backend.database.postgres import async_session_factory

async def test_metrics():
    async with async_session_factory() as db:
        service = ProductivityMetricsService(db=db)
        
        # Mock activities
        activities = [
            {
                'application_name': 'vscode',
                'occurred_at': datetime(2026, 2, 9, 9, 0),
                'duration_seconds': 7200,  # 2 hours
                'activity_type': 'work'
            },
            {
                'application_name': 'zoom',
                'occurred_at': datetime(2026, 2, 9, 11, 0),
                'duration_seconds': 3600,  # 1 hour
                'activity_type': 'meeting'
            },
            {
                'application_name': 'vscode',
                'occurred_at': datetime(2026, 2, 9, 13, 0),
                'duration_seconds': 5400,  # 1.5 hours
                'activity_type': 'work'
            },
        ]
        
        metrics = await service.compute_daily_metrics(
            user_id='test-user-123',
            target_date=date(2026, 2, 9),
            activities=activities
        )
        
        print(f"Focus Score: {metrics.focus_score}")
        print(f"Deep Work: {metrics.deep_work_hours} hours")
        print(f"Meeting Load: {metrics.meeting_load_pct}%")

# Run
import asyncio
asyncio.run(test_metrics())
```

**Expected Output**:
```
Focus Score: 7.2
Deep Work: 3.5 hours
Meeting Load: 28.6%
```

---

## Test 2: Summary Generation

```python
from backend.services.daily_summary_service import DailySummaryService
from backend.services.productivity_metrics_service import ProductivityMetricsService

async def test_summary():
    async with async_session_factory() as db:
        metrics_service = ProductivityMetricsService(db=db)
        summary_service = DailySummaryService(
            db=db,
            metrics_service=metrics_service
        )
        
        # Assumes test_metrics() was run first
        summary = await summary_service.generate_daily_summary(
            user_id='test-user-123',
            target_date=date(2026, 2, 9)
        )
        
        print(f"Summary ({summary.generation_duration_ms}ms):")
        print(summary.summary_markdown)

asyncio.run(test_summary())
```

**Expected Output**:
```
Summary (2500ms):
Today you achieved a focus score of 7.2/10 with 3.5 hours of deep work...
```

---

## Test 3: Email Rendering

```python
from jinja2 import Template

def test_email_render():
    with open('backend/templates/daily_summary_email.html') as f:
        template = Template(f.read())
    
    html = template.render(
        date_formatted='Sunday, February 09, 2026',
        focus_score='7.2',
        focus_color='#f59e0b',  # yellow
        deep_work_hours='3.5',
        meeting_load='29',
        summary_html='<p>Test summary content...</p>',
        dashboard_url='http://localhost:3000/dashboard',
        settings_url='http://localhost:3000/settings',
        unsubscribe_url='http://localhost:3000/unsubscribe'
    )
    
    # Save to file for viewing
    with open('/tmp/test_email.html', 'w') as f:
        f.write(html)
    
    print("Email saved to /tmp/test_email.html")
    print(f"Size: {len(html)} bytes")

test_email_render()
```

**Open in browser**: `open /tmp/test_email.html`

---

## Test 4: Celery Task Execution

```python
from backend.tasks.analytics_tasks import generate_daily_summary_task

# Queue task
result = generate_daily_summary_task.delay(
    'test-user-123',
    '2026-02-09'
)

print(f"Task ID: {result.id}")
print(f"Status: {result.status}")

# Wait for completion
result.get(timeout=60)
print("Task completed!")
```

---

## Test 5: Full Pipeline (End-to-End)

```python
from backend.tasks.analytics_tasks import (
    compute_daily_metrics_task,
    generate_daily_summary_task,
    send_daily_summary_email_task
)

async def test_full_pipeline():
    user_id = 'test-user-123'
    target_date = '2026-02-09'
    
    # Step 1: Compute metrics
    print("1. Computing metrics...")
    metrics_task = compute_daily_metrics_task.delay(user_id, target_date)
    metrics_task.get(timeout=30)
    print("✓ Metrics computed")
    
    # Step 2: Generate summary
    print("2. Generating summary...")
    summary_task = generate_daily_summary_task.delay(user_id, target_date)
    summary_task.get(timeout=60)
    print("✓ Summary generated")
    
    # Step 3: Send email
    print("3. Sending email...")
    email_task = send_daily_summary_email_task.delay(user_id, target_date)
    email_task.get(timeout=30)
    print("✓ Email sent")
    
    print("\n✅ Full pipeline complete!")

asyncio.run(test_full_pipeline())
```

**Expected Output**:
```
1. Computing metrics...
✓ Metrics computed
2. Generating summary...
✓ Summary generated
3. Sending email...
✓ Email sent

✅ Full pipeline complete!
```

---

## Test 6: Scheduled Jobs

```bash
# Start Celery Beat
celery -A celery_app beat --loglevel=info

# Watch logs for scheduled tasks
# Should see at 08:30, 08:45, 09:00 UTC
```

**Expected Logs**:
```
[2026-02-10 08:30:00] analytics.schedule_daily_metrics
[2026-02-10 08:45:00] analytics.schedule_daily_summaries
[2026-02-10 09:00:00] analytics.schedule_daily_emails
```

---

## Performance Validation

### Metrics Computation
- **Target**: < 1s for 1,000 activities
- **Test**:
  ```python
  import time
  
  start = time.time()
  metrics = await service.compute_daily_metrics(user_id, date, activities)
  duration = time.time() - start
  
  print(f"Duration: {duration:.2f}s")
  assert duration < 1.0, f"Too slow: {duration}s"
  ```

### Summary Generation
- **Target**: < 30s (includes LLM call)
- **Test**: Check `generation_duration_ms` in summary object

### Full Pipeline
- **Target**: < 60s total
- **Test**: Time from metrics → email sent

---

## Debugging

### Check Redis Cache

```bash
redis-cli
> KEYS analytics:*
> GET analytics:metrics:test-user-123:2026-02-09
> GET analytics:summary:test-user-123:2026-02-09
```

### Check Database

```sql
-- Daily metrics
SELECT * FROM daily_metrics 
WHERE user_id = 'test-user-123' 
  AND date = '2026-02-09';

-- Daily summaries
SELECT * FROM daily_summaries
WHERE user_id = 'test-user-123'
  AND date = '2026-02-09';

-- Email tracking
SELECT * FROM analytics_emails
WHERE user_id = 'test-user-123'
  AND reference_date = '2026-02-09';
```

### Celery Flower (Monitor)

```bash
pip install flower
celery -A celery_app flower

# Open http://localhost:5555
```

---

## Troubleshooting

### LLM Timeout
```
APITimeoutError: Request timed out
```
**Solution**: Check ANTHROPIC_API_KEY and network

### No Activities
```
ValueError: No metrics found
```
**Solution**: Ensure activities exist for target date

### Email Not Sent
**Solution**: Check email provider credentials

---

## Production Checklist

- [ ] Metrics computed < 1s
- [ ] Summaries generated < 30s
- [ ] Full pipeline < 60s
- [ ] Redis caching working
- [ ] LLM fallback tested
- [ ] Email rendering validated
- [ ] Celery Beat schedule verified
- [ ] Error handling tested
- [ ] Logging comprehensive
- [ ] Performance monitored

---

## Next Steps

Once all tests pass:
1. Deploy to staging
2. Test with real users (beta)
3. Monitor performance
4. Tune LLM prompts based on feedback
5. Deploy to production
