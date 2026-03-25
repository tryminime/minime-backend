#!/usr/bin/env python3
"""
Comprehensive real-data seeder for MiniMe testuser.
Inserts 14 days of realistic activity data: window focus, web visits, IDE sessions,
meetings, emails, social media, document editing etc.
Run: cd /home/ansari/Documents/MiniMe && python3 backend/scripts/seed_real_data.py
"""
import sys
import os
import uuid
from datetime import datetime, timedelta, timezone
import random

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://minime_user:minime_password@localhost:5432/minime_db")

engine = create_engine(DATABASE_URL)

# ── Constants ──────────────────────────────────────────────────────────────────

APPS = {
    "code": [
        ("VS Code", "Visual Studio Code - main.py — MiniMe Backend", None),
        ("VS Code", "Visual Studio Code - page.tsx — MiniMe Frontend", None),
        ("VS Code", "Visual Studio Code - models.py — MiniMe Backend", None),
        ("Terminal", "Terminal — python3 main.py", None),
        ("Terminal", "Terminal — npm run dev", None),
        ("Terminal", "Terminal — git commit -m 'feat: add RAG pipeline'", None),
        ("PyCharm", "PyCharm — MiniMe/backend", None),
    ],
    "browser": [
        ("Chrome", "GitHub — MiniMe/backend PR #42", "github.com"),
        ("Chrome", "Stack Overflow - FastAPI SQLAlchemy async", "stackoverflow.com"),
        ("Chrome", "MDN Web Docs - CSS Grid", "developer.mozilla.org"),
        ("Chrome", "Hacker News", "news.ycombinator.com"),
        ("Chrome", "Google Search - python asyncio best practices", "google.com"),
        ("Chrome", "Twitter / X", "twitter.com"),
        ("Chrome", "YouTube - Clean Architecture FastAPI", "youtube.com"),
        ("Chrome", "Notion — MiniMe Notes", "notion.so"),
        ("Firefox", "localhost:3000 — MiniMe Dashboard", "localhost"),
        ("Firefox", "localhost:8000/docs — FastAPI Swagger", "localhost"),
    ],
    "communication": [
        ("Slack", "Slack — #engineering", None),
        ("Slack", "Slack — #general", None),
        ("Gmail", "Gmail — inbox", "mail.google.com"),
        ("Zoom", "Zoom Meeting — Weekly Standup", None),
        ("Zoom", "Zoom Meeting — Backend Review", None),
        ("Google Meet", "Google Meet — 1:1 with Alex", None),
    ],
    "docs": [
        ("Google Docs", "MiniMe Architecture Spec - Google Docs", "docs.google.com"),
        ("Google Docs", "Sprint Planning Q1 - Google Docs", "docs.google.com"),
        ("Notion", "Notion — Engineering Roadmap", "notion.so"),
        ("Figma", "Figma — MiniMe Design System", "figma.com"),
    ],
}

ACTIVITY_TYPES = {
    "code": "window_focus",
    "browser": "web_visit",
    "communication": "window_focus",
    "docs": "web_visit",
}


def random_duration(category: str) -> int:
    """Return a realistic duration in seconds for the activity category."""
    if category == "code":
        return random.randint(600, 5400)  # 10min – 90min
    elif category == "browser":
        return random.randint(60, 900)    # 1min – 15min
    elif category == "communication":
        return random.randint(120, 3600)  # 2min – 60min
    elif category == "docs":
        return random.randint(300, 2700)  # 5min – 45min
    return 300


def generate_activities(user_id: str, days: int = 14):
    """Generate realistic activities for the past `days` days."""
    activities = []
    now = datetime.now(timezone.utc)

    for day_offset in range(days):
        day = now - timedelta(days=day_offset)
        # Weekdays get more activity
        is_weekday = day.weekday() < 5

        # 8am–7pm work window
        work_start = day.replace(hour=8, minute=0, second=0, microsecond=0)
        work_end   = day.replace(hour=19, minute=0, second=0, microsecond=0)

        # Plan the day's activities
        daily_plan: list[tuple] = []

        # Morning code block (2–4 hours)
        if is_weekday:
            morning_code = random.randint(2, 4)
            for _ in range(morning_code):
                cat = "code"
                app, title, domain = random.choice(APPS[cat])
                daily_plan.append((cat, app, title, domain))

            # Standup / meeting (if weekday, 30% chance)
            if random.random() < 0.7:
                cat = "communication"
                app, title, domain = random.choice(APPS[cat])
                daily_plan.append((cat, app, title, domain))

            # Afternoon browser research
            browser_count = random.randint(3, 8)
            for _ in range(browser_count):
                cat = "browser"
                app, title, domain = random.choice(APPS[cat])
                daily_plan.append((cat, app, title, domain))

            # Afternoon code block
            afternoon_code = random.randint(1, 3)
            for _ in range(afternoon_code):
                cat = "code"
                app, title, domain = random.choice(APPS[cat])
                daily_plan.append((cat, app, title, domain))

            # Docs
            if random.random() < 0.5:
                cat = "docs"
                app, title, domain = random.choice(APPS[cat])
                daily_plan.append((cat, app, title, domain))
        else:
            # Weekend: lighter activity
            weekend_count = random.randint(0, 4)
            for _ in range(weekend_count):
                cat = random.choice(["code", "browser"])
                app, title, domain = random.choice(APPS[cat])
                daily_plan.append((cat, app, title, domain))

        # Place activities on timeline
        current_time = work_start + timedelta(minutes=random.randint(0, 30))
        for cat, app, title, domain in daily_plan:
            dur = random_duration(cat)
            if current_time + timedelta(seconds=dur) > work_end:
                break

            activities.append({
                "id": str(uuid.uuid4()),
                "user_id": user_id,
                "type": ACTIVITY_TYPES.get(cat, "window_focus"),
                "source": "desktop",
                "source_version": "1.0.0",
                "app": app,
                "title": title,
                "domain": domain,
                "url": f"https://{domain}/" if domain else None,
                "duration_seconds": dur,
                "occurred_at": current_time.isoformat(),
                "data": {"category": cat},
                "context": {
                    "category": cat,
                    "productivity_score": round(random.uniform(0.4, 1.0), 2),
                },
                "ingestion_metadata": {"schema_version": "1.0"},
            })

            # Gap between activities (0–15 min)
            gap = random.randint(0, 900)
            current_time += timedelta(seconds=dur + gap)

    return activities


def get_testuser_id(session) -> str | None:
    result = session.execute(text("SELECT id::text FROM users WHERE email = 'testuser@minime.com' LIMIT 1"))
    row = result.fetchone()
    return row[0] if row else None


def clear_existing_activities(session, user_id: str):
    """Remove all existing activities for this user (wipe demo data)."""
    deleted = session.execute(
        text("DELETE FROM activities WHERE user_id = :uid"),
        {"uid": user_id}
    )
    print(f"  Deleted {deleted.rowcount} existing activities")


def insert_activities(session, activities: list):
    """Bulk insert activities."""
    import json as _json
    if not activities:
        return
    for act in activities:
        session.execute(
            text("""
                INSERT INTO activities
                    (id, user_id, type, source, source_version, app, title, domain, url,
                     duration_seconds, occurred_at, data, context, ingestion_metadata)
                VALUES
                    (:id, :user_id, :type, :source, :source_version, :app, :title, :domain, :url,
                     :duration_seconds, :occurred_at,
                     CAST(:data AS jsonb), CAST(:context AS jsonb), CAST(:ingestion_metadata AS jsonb))
                ON CONFLICT (id) DO NOTHING
            """),
            {
                "id": act["id"],
                "user_id": act["user_id"],
                "type": act["type"],
                "source": act["source"],
                "source_version": act["source_version"],
                "app": act["app"],
                "title": act["title"],
                "domain": act["domain"],
                "url": act["url"],
                "duration_seconds": act["duration_seconds"],
                "occurred_at": act["occurred_at"],
                "data": _json.dumps(act["data"]),
                "context": _json.dumps(act["context"]),
                "ingestion_metadata": _json.dumps(act["ingestion_metadata"]),
            }
        )
    print(f"  Inserted {len(activities)} activities")



def seed_goals(session, user_id: str):
    """Insert sample goals if 'goals' table exists."""
    try:
        result = session.execute(text("SELECT COUNT(*) FROM goals WHERE user_id = :uid"), {"uid": user_id})
        count = result.scalar()
        if count > 0:
            print(f"  Goals already exist ({count}), skipping")
            return
        goals = [
            (str(uuid.uuid4()), user_id, "Ship RAG pipeline", "project", "in_progress", 75),
            (str(uuid.uuid4()), user_id, "Write 3 blog posts", "learning", "in_progress", 33),
            (str(uuid.uuid4()), user_id, "Run 5km 3x/week", "wellness", "in_progress", 60),
            (str(uuid.uuid4()), user_id, "Complete FastAPI course", "learning", "completed", 100),
        ]
        for g in goals:
            session.execute(
                text("""
                    INSERT INTO goals (id, user_id, title, category, status, progress_percent, created_at, updated_at)
                    VALUES (:id, :uid, :title, :cat, :status, :prog, now(), now())
                """),
                {"id": g[0], "uid": g[1], "title": g[2], "cat": g[3], "status": g[4], "prog": g[5]}
            )
        print(f"  Inserted {len(goals)} goals")
    except Exception as e:
        print(f"  Goals table not found or error: {e} (skipping)")


def seed_entities(session, user_id: str):
    """Insert sample entities (NER results) if table empty for user."""
    try:
        result = session.execute(text("SELECT COUNT(*) FROM entities WHERE user_id = :uid"), {"uid": user_id})
        count = result.scalar()
        if count > 0:
            print(f"  Entities already exist ({count}), skipping")
            return
        entities = [
            (str(uuid.uuid4()), user_id, "skill", "Python", 0.98, 42),
            (str(uuid.uuid4()), user_id, "skill", "FastAPI", 0.95, 38),
            (str(uuid.uuid4()), user_id, "skill", "React", 0.92, 29),
            (str(uuid.uuid4()), user_id, "skill", "PostgreSQL", 0.88, 22),
            (str(uuid.uuid4()), user_id, "skill", "TypeScript", 0.85, 19),
            (str(uuid.uuid4()), user_id, "project", "MiniMe Backend", 0.99, 85),
            (str(uuid.uuid4()), user_id, "project", "MiniMe Frontend", 0.99, 62),
            (str(uuid.uuid4()), user_id, "concept", "RAG Pipeline", 0.90, 15),
            (str(uuid.uuid4()), user_id, "concept", "Knowledge Graph", 0.87, 12),
            (str(uuid.uuid4()), user_id, "organization", "GitHub", 0.95, 55),
        ]
        for e in entities:
            session.execute(
                text("""
                    INSERT INTO entities (id, user_id, entity_type, name, confidence, occurrence_count, created_at, updated_at)
                    VALUES (:id, :uid, :etype, :name, :conf, :occ, now(), now())
                """),
                {"id": e[0], "uid": e[1], "etype": e[2], "name": e[3], "conf": e[4], "occ": e[5]}
            )
        print(f"  Inserted {len(entities)} entities")
    except Exception as e:
        print(f"  Entities table not found or error: {e} (skipping)")


def main():
    print("=== MiniMe Real Data Seeder ===\n")

    with Session(engine) as session:
        # Get testuser
        user_id = get_testuser_id(session)
        if not user_id:
            print("ERROR: testuser@minime.com not found in DB. Register first.")
            return

        print(f"Found testuser: {user_id}\n")

        # Clear demo data
        print("Step 1: Clearing existing activity data...")
        clear_existing_activities(session, user_id)

        # Generate real activities
        print("\nStep 2: Generating 14 days of realistic activities...")
        activities = generate_activities(user_id, days=14)
        print(f"  Generated {len(activities)} activities")
        insert_activities(session, activities)

        # Seed entities
        print("\nStep 3: Seeding entities (skills, projects, concepts)...")
        seed_entities(session, user_id)

        # Seed goals
        print("\nStep 4: Seeding goals...")
        seed_goals(session, user_id)

        # Commit all
        session.commit()
        print("\n✅ All data committed successfully!")

        # Summary
        result = session.execute(text("SELECT COUNT(*) FROM activities WHERE user_id = :uid"), {"uid": user_id})
        total = result.scalar()
        print(f"\n📊 Final counts for testuser:")
        print(f"  Activities: {total}")

        # Break down by type
        result = session.execute(
            text("SELECT type, COUNT(*) FROM activities WHERE user_id = :uid GROUP BY type ORDER BY COUNT(*) DESC"),
            {"uid": user_id}
        )
        for row in result:
            print(f"    {row[0]}: {row[1]}")

        # Break down by app
        result = session.execute(
            text("SELECT app, SUM(duration_seconds)/3600.0 as hours FROM activities WHERE user_id = :uid AND app IS NOT NULL GROUP BY app ORDER BY hours DESC LIMIT 10"),
            {"uid": user_id}
        )
        print(f"\n  Top apps by hours:")
        for row in result:
            print(f"    {row[0]}: {row[1]:.1f}h")


if __name__ == "__main__":
    main()
