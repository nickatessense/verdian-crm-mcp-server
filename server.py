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

- salesforce_opportunities_report (468 rows): Salesforce integration data.
  - Salesforce opportunity and contact data for renewal tracking

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
    self_service_admins, and salesforce_opportunities_report tables.

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
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route, Mount

    port = int(os.environ.get('PORT', 8000))

    # Health endpoint for Railway
    async def health(request):
        return JSONResponse({"status": "ok", "service": "verdian-crm-mcp"})

    # Get the MCP ASGI app
    mcp_app = mcp.http_app()

    # Combine health + MCP into one Starlette app
    app = Starlette(
        routes=[
            Route("/health", health),
            Mount("/", app=mcp_app),
        ]
    )

    uvicorn.run(app, host="0.0.0.0", port=port)
