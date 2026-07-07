# Snowflake-Data-Engineering-Sample-Project

# 🎮 Snowflake Data Engineering End-to-End Pipeline

An end-to-end **Data Engineering project** built in **Snowflake** demonstrating how to ingest semi-structured JSON logs, transform and enrich the data, automate data pipelines using **Tasks**, **Snowpipe**, and **Streams (CDC)**, and build curated datasets for analytics.

---

# 📖 Project Overview

This project simulates a gaming platform that collects player activity logs.

The pipeline performs the following:

* Ingest JSON game logs from an external stage
* Parse and transform semi-structured data
* Enrich data with Geo-IP location information
* Convert timestamps into players' local time zones
* Build an Enhanced analytics layer
* Automate ingestion using Snowpipe
* Detect new records using Streams (CDC)
* Merge incremental data into analytics tables
* Create Curated reporting tables for dashboards

---

# 🏗️ Architecture

```text
                JSON Files
                    │
                    ▼
             External Stage (S3)
                    │
                    ▼
               Snowpipe (Auto Load)
                    │
                    ▼
          RAW.ED_PIPELINE_LOGS
                    │
                    ▼
              Stream (CDC)
                    │
                    ▼
              Task (MERGE)
                    │
                    ▼
       ENHANCED.LOGS_ENHANCED
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
 GAMER_LOGIN_LOGOUT      GAMER_TIME
                │
                ▼
          SESSION_LENGTH
```

---

# 📂 Database Structure

```
AGS_GAME_AUDIENCE
│
├── RAW
│   ├── GAME_LOGS
│   ├── PL_GAME_LOGS
│   ├── ED_PIPELINE_LOGS
│   ├── LOGS (VIEW)
│   ├── PL_LOGS (VIEW)
│   ├── TIME_OF_DAY_LU
│   └── Streams / Tasks
│
├── ENHANCED
│   ├── LOGS_ENHANCED
│   └── LOGS_ENHANCED_BU
│
└── CURATED
    ├── GAMER_LOGIN_LOGOUT
    ├── GAMER_TIME
    └── SESSION_LENGTH
```

---

# ⚙️ Technologies Used

* Snowflake
* SQL
* Snowpipe
* Streams
* Tasks
* Time Travel
* Zero Copy Cloning
* MERGE
* COPY INTO
* Semi-Structured Data (JSON)
* VARIANT Data Type
* Window Functions
* ListAgg
* External Stage (AWS S3)
* Geo-IP Lookup (IPINFO)

---

# 🚀 Project Workflow

## 1. Security & Environment Setup

* Configure roles
* Transfer ownership
* Configure warehouse
* Configure users
* Set default warehouse, role, and schema

Example:

```sql
USE ROLE ACCOUNTADMIN;

GRANT OWNERSHIP ON WAREHOUSE COMPUTE_WH
TO ROLE SYSADMIN
COPY CURRENT GRANTS;
```

---

# 2. Create Database

```sql
CREATE DATABASE AGS_GAME_AUDIENCE;

CREATE SCHEMA RAW;
CREATE SCHEMA ENHANCED;
CREATE SCHEMA CURATED;
```

---

# 3. Load JSON Files

Create a JSON File Format

```sql
CREATE FILE FORMAT FF_JSON_LOGS
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE;
```

Load files

```sql
COPY INTO GAME_LOGS
FROM @UNI_KISHORE/KICKOFF
FILE_FORMAT=(FORMAT_NAME=FF_JSON_LOGS);
```

---

# 4. Parse JSON Data

The raw logs are stored as a VARIANT column.

Example:

```sql
SELECT
RAW_LOG:"user_login"::STRING,
RAW_LOG:"datetime_iso8601"::TIMESTAMP_NTZ,
RAW_LOG:"user_event"::STRING
FROM GAME_LOGS;
```

---

# 5. Build Views

Create views that cast JSON fields into relational columns.

```sql
CREATE VIEW LOGS AS
SELECT
RAW_LOG:"ip_address"::VARCHAR AS IP_ADDRESS,
RAW_LOG:"user_login"::STRING,
RAW_LOG:"user_event"::STRING,
RAW_LOG:"datetime_iso8601"::TIMESTAMP_NTZ
FROM GAME_LOGS;
```

---

# 6. Geo-IP Enrichment

Using the shared IPINFO dataset:

* City
* Region
* Country
* Timezone

```sql
JOIN IPINFO_GEOLOC.DEMO.LOCATION
```

Convert timestamps into each player's local timezone.

```sql
CONVERT_TIMEZONE(
'UTC',
timezone,
datetime_iso8601
)
```

---

# 7. Enhanced Layer

Create a CTAS table that joins:

* Logs
* Geo Location
* Time of Day Lookup

Result:

```
ENHANCED.LOGS_ENHANCED
```

Columns include:

* Gamer Name
* IP Address
* Country
* Region
* City
* Local Time
* Day Name
* Time of Day

---

# 8. Incremental Loading Using MERGE

Instead of reloading everything:

```sql
MERGE INTO LOGS_ENHANCED
USING (...)
```

Benefits:

* Prevent duplicates
* Load only new records
* Efficient incremental processing

---

# 9. Automate with Tasks

Create scheduled Tasks.

Example:

```sql
CREATE TASK LOAD_LOGS_ENHANCED
SCHEDULE='5 MINUTES'
AS
MERGE ...
```

Resume

```sql
ALTER TASK LOAD_LOGS_ENHANCED RESUME;
```

---

# 10. Snowpipe

Automatically ingest files arriving in S3.

```sql
CREATE PIPE PIPE_GET_NEW_FILES
AUTO_INGEST=TRUE
AS
COPY INTO ED_PIPELINE_LOGS
...
```

Pipeline:

```
S3
 ↓
SNS
 ↓
Snowpipe
 ↓
Snowflake Table
```

---

# 11. Change Data Capture (CDC)

Create a Stream

```sql
CREATE STREAM ED_CDC_STREAM
ON TABLE ED_PIPELINE_LOGS;
```

Check whether new data exists

```sql
SELECT SYSTEM$STREAM_HAS_DATA('ED_CDC_STREAM');
```

---

# 12. Event Driven Pipeline

The pipeline only executes when:

* New files arrive
* Stream contains data

```sql
WHEN
SYSTEM$STREAM_HAS_DATA('ED_CDC_STREAM')
```

This avoids unnecessary warehouse usage.

---

# 13. Curated Analytics Layer

### Gamer Login History

Uses `LISTAGG`

```sql
LISTAGG(GAME_EVENT_LTZ,' / ')
```

Produces:

```
Player A

09:15 Login
10:02 Logout
13:21 Login
15:11 Logout
```

---

### Gamer Session Time

Uses Window Function

```sql
LEAD()
```

Calculates:

```
Login
Logout
Minutes Played
```

---

### Session Length Heatmap

Groups sessions into buckets

```
<10 mins

10-19 mins

20-29 mins

30-39 mins

40+ mins
```

Perfect for dashboards.

---

# 📊 Final Curated Tables

| Table              | Description            |
| ------------------ | ---------------------- |
| GAMER_LOGIN_LOGOUT | Login & logout history |
| GAMER_TIME         | Session duration       |
| SESSION_LENGTH     | Heatmap buckets        |

---

# 🔄 Complete Pipeline Flow

```text
JSON Logs
      │
      ▼
External Stage
      │
      ▼
Snowpipe
      │
      ▼
RAW Tables
      │
      ▼
Views
      │
      ▼
Geo-IP Enrichment
      │
      ▼
Enhanced Table
      │
      ▼
CDC Stream
      │
      ▼
Merge Task
      │
      ▼
Curated Tables
      │
      ▼
Dashboard
```

---

# ✨ Snowflake Features Demonstrated

* ✅ Roles & Security
* ✅ Warehouses
* ✅ Databases & Schemas
* ✅ External Stages
* ✅ JSON File Formats
* ✅ COPY INTO
* ✅ VARIANT Data Type
* ✅ JSON Parsing
* ✅ Views
* ✅ CTAS
* ✅ MERGE
* ✅ Window Functions
* ✅ LISTAGG
* ✅ Time Zone Conversion
* ✅ Zero Copy Cloning
* ✅ Time Travel
* ✅ Streams (CDC)
* ✅ Snowpipe
* ✅ Tasks
* ✅ Serverless Tasks
* ✅ Event-Driven Pipelines

---

# 📈 Skills Demonstrated

* Data Engineering
* ELT Pipeline Development
* Incremental Data Loading
* Change Data Capture (CDC)
* Semi-Structured Data Processing
* SQL Optimization
* Snowflake Administration
* Data Modeling
* Workflow Automation
* Data Enrichment
* Pipeline Orchestration





## ⭐ If you found this project helpful, consider giving the repository a star!
