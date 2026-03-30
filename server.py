"""
Verdian CRM Database MCP Server

Remote MCP server that gives Claude.ai read-only access to the
Verdian CRM PostgreSQL database. Deploy on Railway and connect
as a custom integration in Claude Teams.
"""

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from fastmcp import FastMCP

# Database configuration
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'interchange.proxy.rlwy.net'),
    'port': int(os.environ.get('DB_PORT', '31251')),
    'database': os.environ.get('DB_NAME', 'advance_test'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', ''),
}

# Initialize MCP server
mcp = FastMCP(
    "Verdian CRM Database",
    instructions="""You are connected to the Verdian CRM PostgreSQL database.
This database contains data migrated from Abacus ADvance CRM for Compliance Week.

## Tables Available
- contacts (103,824 rows): Master party table. party_id PK. Contains both people AND companies.
  - people_party_id: if NOT NULL = person, if NULL = company
  - Key fields: first_name, surname, email, phone, title, company_name, company_id, address, people_status

- subscriptions (72,321 rows): One row per subscription per recipient.
  - party_id FK, subscription_status, start_date, end_date, subscription_sub_type
  - quantity, product_id, product_name, product_type, order_id, order_type, order_status
  - Starting from Subscriptions folder = RECIPIENTS (who gets access)

- engagement (66,290 rows): Engagement metrics per person (Compliance Week brand only).
  - party_id, period_type (All Time / Month), engagement_index, quartile, decile
  - online_rank, online_count, frequency_rank, frequency_ratio
  - recency_rank, recency_days, first_visited, last_visited, days_visited
  - story_views, page_views, brand_registration_status, party_registration_status

- web_registrations (70,329 rows): Web registration per person.
  - party_id, is_activated (Activated/Not activated), registration_date

- newsletter_subscriptions (52,699 rows): One row per person per newsletter.
  - party_id, newsletter_name
  - 7 newsletters: Compliance Week News, Thought Leadership, Announcements, Financial Services, Special Offers, Events, Content Alerts

- custom_attributes (62,929 rows): User preferences and interests.
  - party_id, attribute_name, attribute_value, category, date_set
  - Categories: Global, User content preferences
  - Key attributes: AreasofInterest, Topics, Industries, Regions, Job Function, Job Level

- self_service_admins (2,196 rows): Corporate account admin assignments.
  - company_party_id, company_name, admin_party_id, admin_name, order_id, record_status

- customer_diary (456,512 rows): Subscription lifecycle audit trail.
  - party_id, event_date, event_type, item, actioned_by, product_name
  - Key event types: Corporate subscription activated, Party removed from corporate subscription,
    Corporate subscription, Personal subscription, Order status changed,
    Order flagged as do not allow payment/renew, Newsletter registration/cancellation,
    Form completed, Refund, Order extension, Party merged, Credit Card Updated
  - The "item" field contains details like order numbers and who performed the action

- web_activity (1,063,652 rows): Web activity events per person.
  - party_id, event_datetime (TIMESTAMP), item_title, item_type, action
  - Actions: View Story (478K — articles read), View Page (500K — pages visited), Access Denied (67K — paywall hits), Download (18K)
  - View Story available from 2020 onwards only (not tracked before 2020)
  - View Page available from 2022 onwards only (not tracked before 2022)
  - Access Denied and Download available from 2019 onwards
  - Login and Access Granted actions NOT imported (Login = noise/auto-logins, Access Granted = duplicate of View Story)

- css_invitations (17,614 rows): Corporate Self-Service invitation history.
  - date_sent, order_id, party_id (null for TEMP PARTY), name, email, company_party_id, company_name, activation_type
  - Activation types: "Unknown invitee (TEMP PARTY)" = invited but never created account,
    "Invitee on system (PARTY/PERSON)" = exists in system but not confirmed,
    "MM/DD/YYYY Confirmed" = accepted invitation on that date
  - Same person can appear multiple times across orders (invitation history through renewals)
  - Use this + subscriptions to get FULL picture: subscriptions = active recipients, css_invitations = invited but not yet active
  - TEMP PARTY invitees have NO party_id — only name and email

## Key Business Rules
- party_id represents BOTH people and companies (universal identifier)
- To check if person or company: people_party_id IS NOT NULL = person, IS NULL = company
- Subscriptions show RECIPIENTS (who receives access)
- Orders show BILL PAYERS (who pays) — orders table not yet imported
- Corporate orders sit on the company's party_id, individuals are recipients
- subscription_status values: Live, Expired, Cancelled, Suspended, Deleted, Goneaway, Pro-forma, Draft
- people_status values: Live, Inactive, Deceased, Merged, Deleted
- All data is for one brand: Compliance Week
- engagement period_type: "All Time" (lifetime) or "Month" (current month)
- self_service_admins links admins to corporate orders they manage

## Common Query Patterns
- Find person by email: SELECT * FROM contacts WHERE email = 'x@y.com'
- Find subscriptions: JOIN contacts c ON s.party_id = c.party_id
- Corporate recipients for an order: SELECT c.* FROM subscriptions s JOIN contacts c ON s.party_id = c.party_id WHERE s.order_id = X
- Find admins for a company: SELECT * FROM self_service_admins WHERE company_name ILIKE '%name%'
- Live subscribers: WHERE subscription_status = 'Live'
- Companies only: WHERE people_party_id IS NULL
- People only: WHERE people_party_id IS NOT NULL
- Diary for a person: SELECT * FROM customer_diary WHERE party_id = X ORDER BY event_date DESC
- Why was someone removed: SELECT * FROM customer_diary WHERE party_id = X AND event_type LIKE '%removed%'
- Corporate additions/removals for an order: SELECT * FROM customer_diary WHERE item LIKE '%#0000007263%' AND event_type IN ('Corporate subscription activated', 'Party removed from corporate subscription')
- What articles did a person read: SELECT event_datetime, item_title FROM web_activity WHERE party_id = X AND action = 'View Story' ORDER BY event_datetime DESC LIMIT 50
- What content was someone denied: SELECT event_datetime, item_title FROM web_activity WHERE party_id = X AND action = 'Access Denied' ORDER BY event_datetime DESC
- Most read articles: SELECT item_title, COUNT(*) FROM web_activity WHERE action = 'View Story' GROUP BY item_title ORDER BY COUNT(*) DESC LIMIT 20
- ALL users for a corporate order (active + invited): SELECT name, email, activation_type FROM css_invitations WHERE order_id = X UNION ALL SELECT c.first_name || ' ' || c.surname, c.email, 'Active Subscriber' FROM subscriptions s JOIN contacts c ON s.party_id = c.party_id WHERE s.order_id = X AND c.people_party_id IS NOT NULL
- Ghost invitees for a company: SELECT * FROM css_invitations WHERE company_name ILIKE '%name%' AND activation_type = 'Unknown invitee (TEMP PARTY)'
"""
)


def get_db_connection():
    """Create a new database connection."""
    return psycopg2.connect(**DB_CONFIG)


@mcp.tool
def run_sql(query: str) -> str:
    """
    Execute a READ-ONLY SQL query against the Verdian CRM PostgreSQL database.

    Only SELECT queries are allowed. The database contains contacts, subscriptions,
    engagement, web_registrations, newsletter_subscriptions, custom_attributes,
    self_service_admins, customer_diary, web_activity, and css_invitations tables.

    Args:
        query: SQL SELECT query to execute. Must be read-only.

    Returns:
        Query results as JSON array, or error message.
    """
    # Safety: only allow SELECT queries
    cleaned = query.strip().upper()
    if not cleaned.startswith('SELECT') and not cleaned.startswith('WITH') and not cleaned.startswith('EXPLAIN'):
        return "Error: Only SELECT, WITH (CTE), and EXPLAIN queries are allowed. This is a read-only connection."

    # Block dangerous keywords
    dangerous = ['DELETE', 'DROP', 'TRUNCATE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'GRANT', 'REVOKE']
    for word in dangerous:
        if word in cleaned.split():
            return f"Error: {word} operations are not allowed. This is a read-only connection."

    try:
        conn = get_db_connection()
        conn.set_session(readonly=True, autocommit=True)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Add LIMIT if not present (safety for large tables)
        if 'LIMIT' not in cleaned and not cleaned.startswith('EXPLAIN'):
            query = query.rstrip(';') + ' LIMIT 500;'

        cur.execute(query)
        rows = cur.fetchall()

        # Convert to serializable format
        result = []
        for row in rows:
            clean_row = {}
            for key, value in row.items():
                if value is None:
                    clean_row[key] = None
                elif hasattr(value, 'isoformat'):
                    clean_row[key] = value.isoformat()
                else:
                    clean_row[key] = str(value) if not isinstance(value, (int, float, bool)) else value
            result.append(clean_row)

        cur.close()
        conn.close()

        row_count = len(result)
        if row_count == 0:
            return "Query returned 0 rows."

        return json.dumps(result, indent=2, ensure_ascii=False)

    except psycopg2.Error as e:
        return f"Database error: {str(e)}"
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def list_tables() -> str:
    """
    List all available tables in the database with their row counts.

    Returns:
        Table names and row counts.
    """
    try:
        conn = get_db_connection()
        conn.set_session(readonly=True, autocommit=True)
        cur = conn.cursor()
        cur.execute("""
            SELECT schemaname, relname AS table_name, n_live_tup AS row_count
            FROM pg_stat_user_tables
            WHERE relname NOT LIKE 'archive_%'
            ORDER BY n_live_tup DESC;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        result = []
        for schema, table, count in rows:
            result.append(f"{table}: {count:,} rows")

        return "\n".join(result)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def describe_table(table_name: str) -> str:
    """
    Show the columns, types, and constraints for a specific table.

    Args:
        table_name: Name of the table to describe.

    Returns:
        Column definitions for the table.
    """
    # Prevent SQL injection
    if not table_name.replace('_', '').isalnum():
        return "Error: Invalid table name."

    try:
        conn = get_db_connection()
        conn.set_session(readonly=True, autocommit=True)
        cur = conn.cursor()
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position;
        """, (table_name,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            return f"Table '{table_name}' not found."

        result = [f"Table: {table_name}\n"]
        for col_name, data_type, nullable, default in rows:
            null_str = "NULL" if nullable == 'YES' else "NOT NULL"
            default_str = f" DEFAULT {default}" if default else ""
            result.append(f"  {col_name}: {data_type} {null_str}{default_str}")

        return "\n".join(result)
    except Exception as e:
        return f"Error: {str(e)}"


if __name__ == "__main__":
    import uvicorn
    from starlette.responses import JSONResponse

    port = int(os.environ.get('PORT', 8000))

    # Get the MCP ASGI app (serves on /mcp/)
    app = mcp.http_app()

    # Add health endpoint directly to the underlying Starlette app
    original_app = app

    async def wrapped_app(scope, receive, send):
        if scope["type"] == "http" and scope["path"] == "/health":
            response = JSONResponse({"status": "ok", "service": "verdian-crm-mcp"})
            await response(scope, receive, send)
        else:
            await original_app(scope, receive, send)

    uvicorn.run(wrapped_app, host="0.0.0.0", port=port)
