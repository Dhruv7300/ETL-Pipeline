# ETL Pipeline Implementation Plan
## Real-time Event Processing for Uber-like Platform

---

## 1. Executive Summary

This document outlines the implementation plan for a proof-of-concept (POC) ETL (Extract, Transform, Load) pipeline designed to process real-time events from a WebSocket stream. The pipeline will handle events from a ride-sharing platform (Uber-like domain) including new vehicles, services, drivers, and trip completions.

**Key Highlights:**
- Real-time event streaming via WebSocket
- Self-healing architecture with Dead Letter Queue (DLQ)
- Schema evolution support for future changes
- PostgreSQL as the target database

---

## 2. Problem Statement

The organization needs to:
1. **Ingest real-time events** from a WebSocket stream without missing data
2. **Process diverse event types** (vehicles, services, trips, etc.) reliably
3. **Handle schema changes** when new features are added (e.g., new vehicle types, payment methods)
4. **Ensure pipeline never breaks** - a single bad event should not stop processing
5. **Maintain data quality** by isolating invalid records for later analysis

---

## 3. Proposed Architecture

### 3.1 High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              PIPELINE ARCHITECTURE                              │
└─────────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
    │   WebSocket  │      │   Ingestion  │      │   Transform  │      │  PostgreSQL  │
    │    Stream    │────▶│    Layer     │─────▶│    Layer     │────▶│  Database    │
    └──────────────┘      └──────────────┘      └──────────────┘      └──────────────┘
                                 │                    │                              │
                                 │             ┌──────┴──────┐                       │
                                 │             │             │                       │
                                 │         [Schema       [Event                      │
                                 │        Validation]    Handlers]                   │
                                 │                                                   │
                                 │              ┌─────────────┐                      │
                                 └────────────▶│     DLQ     │                      │
                                                │  (Dead      │                      │
                                                │ Letter      │                      │
                                                │  Queue)     │                      │
                                                └─────────────┘                      │
                                                                                     │
                                    SELF-HEALING MECHANISMS                            
                                    ─────────────────────────                        
                                    • Auto-reconnect on disconnect                  
                                    • Retry with exponential backoff               
                                    • Invalid events → DLQ (don't break)            
                                    • DB failures → DLQ + continue                 
```

### 3.2 End-to-End Event Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          VALID EVENT PROCESSING FLOW                            │
└─────────────────────────────────────────────────────────────────────────────────┘

   [WebSocket]        [Ingestion]       [Transform]        [Load]         [Database]
       │                 │                 │                 │                 │
       │ {event_type:    │                 │                 │                 │
       │  "trip_         │                 │                 │                 │
       │  completed",    │                 │                 │                 │
       │  ...}           │                 │                 │                 │
       │───────────────▶│ Parse JSON      │                 │                 │        
       │                │───────────────▶ │ Validate Schema │                 │
       │                │                  │───────────────▶│ Route to        │
       │                │                  │                 │ Handler         │
       │                │                  │                 │──────────────▶ │
       │                │                  │                 │        INSERT   │
       │                │                  │                 │      ────────▶ |
       │                │                  │                 │                 │
       │                │                  │   [Success]     |   [trips table] |
       │                │                  │                 │                 │
       ▼                ▼                  ▼                 ▼                 ▼
    Event JSON    Valid Dict      Validated Dict   DB Write       Data Stored
```

### 3.3 Error Handling Flow (Self-Healing)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       INVALID EVENT → DLQ FLOW                                  │
└─────────────────────────────────────────────────────────────────────────────────┘

   [WebSocket]        [Ingestion]       [Validation]       [DLQ]         [Next Event]
       │                 │                 │                │                 │
       │ {event_type:    │                 │                │                 │
       │  "trip_         │                 │                │                 │
       │  completed",    │                 │                │                 │
       │  fare: "abc"}   │───────────────▶│ Check required │                  │
       │  (invalid!)     │                 │ fields & types │                 │
       │                 │                 │──────────────▶│ Route to DLQ     │
       │                 │                 │                │───────────────▶ │
       │                 │                 │                │        INSERT   │
       │                 │                 │                │───────────────▶│
       │                 │                 │                │                 │
       │                 │                 │         [dead_letter   [continues]
       │                 │                 │          _queue]      
       │                 │                 │                             
       ▼                 ▼                 ▼                ▼                 ▼
    Event JSON    Valid Dict      Validation Failed  Logged + Alert   Pipeline OK!
```

---

## 4. Technology Stack

| Layer | Technology | Justification |
|-------|------------|---------------|
| **Ingestion** | `websocket-client` (Python) | Pure Python, no native dependencies, auto-reconnect support |
| **Validation** | Custom Python + `pydantic` | Type-safe schema validation |
| **Database Driver** | `psycopg2-binary` | Easy install, PostgreSQL native support |
| **Retry Logic** | `tenacity` | Exponential backoff, declarative retries |
| **Configuration** | `python-dotenv` | Environment-based config |
| **Database** | PostgreSQL | Your existing target |

### Python Dependencies
```
websocket-client>=1.0.0
psycopg2-binary>=2.9.0
pydantic>=2.0.0
tenacity>=8.0.0
python-dotenv>=1.0.0
```

---

## 5. Database Schema

### 5.1 Core Tables (Event-Type Specific)

```sql
-- Vehicles (new_vehicle events)
CREATE TABLE vehicles (
    id UUID PRIMARY KEY,
    vehicle_type VARCHAR(50),
    model VARCHAR(100),
    license_plate VARCHAR(50),
    added_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB
);

-- Services (new_service events)
CREATE TABLE services (
    id UUID PRIMARY KEY,
    service_name VARCHAR(100),
    service_type VARCHAR(50),
    base_fare DECIMAL(10,2),
    per_km_rate DECIMAL(10,2),
    launched_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB
);

-- Drivers (new_driver events)
CREATE TABLE drivers (
    id UUID PRIMARY KEY,
    name VARCHAR(100),
    license_number VARCHAR(100),
    rating DECIMAL(3,2),
    status VARCHAR(20),
    vehicle_id UUID REFERENCES vehicles(id),
    created_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB
);

-- Riders (rider account events)
CREATE TABLE riders (
    id UUID PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    rating DECIMAL(3,2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB
);

-- Rides (ride_request / ride_started / ride_completed events)
CREATE TABLE rides (
    id UUID PRIMARY KEY,
    rider_id UUID REFERENCES riders(id),
    driver_id UUID REFERENCES drivers(id),
    service_id UUID REFERENCES services(id),
    pickup_location JSONB,
    dropoff_location JSONB,
    status VARCHAR(50),  -- requested, accepted, started, completed, cancelled
    distance_km DECIMAL(10,2),
    fare DECIMAL(10,2),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    metadata JSONB
);

-- Payments (payment_completed events)
CREATE TABLE payments (
    id UUID PRIMARY KEY,
    ride_id UUID REFERENCES rides(id),
    rider_id UUID REFERENCES riders(id),
    amount DECIMAL(10,2),
    currency VARCHAR(3),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),  -- pending, completed, failed, refunded
    transaction_id VARCHAR(100),
    paid_at TIMESTAMP,
    metadata JSONB
);

-- Fare Changes (fare_changed events)
CREATE TABLE fare_changes (
    id UUID PRIMARY KEY,
    service_id UUID REFERENCES services(id),
    old_fare DECIMAL(10,2),
    new_fare DECIMAL(10,2),
    effective_from TIMESTAMP,
    changed_at TIMESTAMP DEFAULT NOW()
);

-- Dead Letter Queue (invalid events)
CREATE TABLE dead_letter_queue (
    id SERIAL PRIMARY KEY,
    raw_event JSONB NOT NULL,
    error_message TEXT,
    received_at TIMESTAMP DEFAULT NOW()
);
```

### 5.2 Table Relationships

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            TABLE RELATIONSHIPS                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

                        ┌─────────────┐
                        │  services   │
                        │     (PK)    │
                        └──────┬──────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
              ┌─────┴─────┐         ┌─────┴─────┐
              │ vehicles  │         │fare_change│
              │   (PK)    │         │    (PK)   │
              └─────┬─────┘         └───────────┘
                    │
              ┌─────┴─────┐
              │ drivers   │
              │    (FK)  │
              │ vehicle  │
              └─────┬─────┘
                    │
              ┌─────┴─────┐
              │   rides   │
              │   (FK)    │
              │ driver,   │
              │ rider,    │
              │ service   │
              └─────┬─────┘
                    │
              ┌─────┴─────┐
              │ payments  │
              │  (FK)     │
              │ ride,     │
              │ rider     │
              └───────────┘

    riders ──────────────────────────▶ rides (rider initiates ride)
    riders ──────────────────────────▶ payments (rider pays for ride)
```

### 5.3 Event Types Mapping

| Event Type | Target Table |
|------------|--------------|
| `new_vehicle` | vehicles |
| `new_service` | services |
| `new_driver` | drivers |
| `new_rider` | riders |
| `ride_requested` | rides |
| `ride_started` | rides (update) |
| `trip_completed` | rides (update) |
| `payment_completed` | payments |
| `fare_changed` | fare_changes |

---

## 6. Supported Event Types

| Event Type | Description | Required Fields |
|------------|-------------|------------------|
| `new_vehicle` | New vehicle added to fleet | vehicle_id, type, model, license_plate |
| `new_service` | New service launched | service_id, name, service_type |
| `new_driver` | New driver registered | driver_id, name, license_number, rating, vehicle_id |
| `new_rider` | New rider account created | rider_id, name, email, phone |
| `ride_requested` | Rider requests a ride | ride_id, rider_id, driver_id, service_id, pickup, dropoff |
| `ride_started` | Driver starts the ride | ride_id, driver_id, started_at |
| `trip_completed` | Trip finished | ride_id, distance_km, fare, completed_at |
| `payment_completed` | Payment processed | payment_id, ride_id, rider_id, amount, payment_method |
| `fare_changed` | Fare rules updated | service_id, old_fare, new_fare |

---

## 7. Self-Healing Mechanisms

| Mechanism | Description | Behavior on Failure |
|-----------|-------------|---------------------|
| **Auto-Reconnect** | WebSocket disconnects | Exponential backoff: 1s → 2s → 4s → 8s → max 60s |
| **Schema Validation** | Invalid event structure | Route to DLQ, log error, continue processing |
| **Unknown Events** | New event type received | Route to DLQ as "unhandled", continue pipeline |
| **DB Write Failure** | PostgreSQL unavailable | Retry once, then route to DLQ, continue |
| **Missing Fields** | Required fields missing | Route to DLQ with specific error, continue |

**Core Principle**: One bad event should NEVER stop the entire pipeline.

---

## 8. Schema Evolution Strategy

### 8.1 Safe vs Breaking Changes

| Safe (Backward-Compatible) | Breaking |
|----------------------------|----------|
| Adding new columns | Removing columns |
| Adding nullable columns | Renaming columns |
| Type widening (INT → BIGINT) | Type narrowing (STRING → INT) |

### 8.2 Implementation

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         SCHEMA EVOLUTION HANDLING                              │
└─────────────────────────────────────────────────────────────────────────────────┘

    [Incoming Event]                    [Pipeline Action]
    ──────────────────                  ─────────────────
    
    New optional field ────────────────▶ Add to metadata JSONB column
    New required field ────────────────▶ Alert + route to DLQ (for review)
    Removed field ─────────────────────▶ Route to DLQ + Alert
    
    Result: Pipeline continues, no downtime, full audit trail
```

---

*Document Version: 1.0*
*Created: April 2026*
