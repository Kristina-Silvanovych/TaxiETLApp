IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TaxiData')
BEGIN
    CREATE DATABASE TaxiData;
END;
GO

USE TaxiData;
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TaxiTrips')
BEGIN
    CREATE TABLE TaxiTrips
    (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        tpep_pickup_datetime DATETIME NOT NULL,
        tpep_dropoff_datetime DATETIME NOT NULL,
        passenger_count INT NOT NULL,
        trip_distance FLOAT NOT NULL,
        store_and_fwd_flag VARCHAR(3) NOT NULL,
        PULocationID INT NOT NULL,
        DOLocationID INT NOT NULL,
        fare_amount DECIMAL(10,2) NOT NULL,
        tip_amount DECIMAL(10,2) NOT NULL
    );
END;
GO

IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_tip_amount' AND object_id = OBJECT_ID('TaxiTrips'))
    DROP INDEX idx_tip_amount ON TaxiTrips;
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_trip_distance' AND object_id = OBJECT_ID('TaxiTrips'))
    DROP INDEX idx_trip_distance ON TaxiTrips;
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_pickup_time' AND object_id = OBJECT_ID('TaxiTrips'))
    DROP INDEX idx_pickup_time ON TaxiTrips;
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_pulocation' AND object_id = OBJECT_ID('TaxiTrips'))
    DROP INDEX idx_pulocation ON TaxiTrips;
GO

CREATE NONCLUSTERED INDEX idx_pulocation ON TaxiTrips (PULocationID) INCLUDE (tip_amount);
CREATE NONCLUSTERED INDEX idx_trip_distance ON TaxiTrips (trip_distance DESC);
CREATE NONCLUSTERED INDEX idx_pickup_dropoff ON TaxiTrips (tpep_pickup_datetime, tpep_dropoff_datetime);
GO