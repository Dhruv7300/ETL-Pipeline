-- Core schema with foreign key relationships

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Services
CREATE TABLE IF NOT EXISTS services (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    service_name VARCHAR(100) NOT NULL,
    service_type VARCHAR(50) NOT NULL,
    base_fare DECIMAL(10,2),
    per_km_rate DECIMAL(10,2),
    launched_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'
);

-- Vehicles
CREATE TABLE IF NOT EXISTS vehicles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vehicle_type VARCHAR(50) NOT NULL,
    model VARCHAR(100),
    license_plate VARCHAR(50),
    added_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'
);

-- Drivers
CREATE TABLE IF NOT EXISTS drivers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    license_number VARCHAR(100),
    rating DECIMAL(3,2),
    status VARCHAR(20) DEFAULT 'active',
    vehicle_id UUID REFERENCES vehicles(id),
    created_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'
);

-- Riders
CREATE TABLE IF NOT EXISTS riders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    rating DECIMAL(3,2),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'
);

-- Rides
CREATE TABLE IF NOT EXISTS rides (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rider_id UUID REFERENCES riders(id) NOT NULL,
    driver_id UUID REFERENCES drivers(id),
    service_id UUID REFERENCES services(id),
    pickup_location JSONB,
    dropoff_location JSONB,
    status VARCHAR(50) DEFAULT 'requested',
    distance_km DECIMAL(10,2),
    fare DECIMAL(10,2),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    metadata JSONB DEFAULT '{}'
);

-- Payments
CREATE TABLE IF NOT EXISTS payments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ride_id UUID REFERENCES rides(id) NOT NULL,
    rider_id UUID REFERENCES riders(id) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    payment_method VARCHAR(50),
    payment_status VARCHAR(50) DEFAULT 'pending',
    transaction_id VARCHAR(100),
    paid_at TIMESTAMP,
    metadata JSONB DEFAULT '{}'
);

-- Fare Changes
CREATE TABLE IF NOT EXISTS fare_changes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    service_id UUID REFERENCES services(id) NOT NULL,
    old_fare DECIMAL(10,2),
    new_fare DECIMAL(10,2),
    effective_from TIMESTAMP,
    changed_at TIMESTAMP DEFAULT NOW()
);

-- Dead Letter Queue
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id SERIAL PRIMARY KEY,
    raw_event JSONB NOT NULL,
    error_message TEXT,
    received_at TIMESTAMP DEFAULT NOW()
);