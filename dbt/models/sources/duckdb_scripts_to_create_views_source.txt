
-- Ensure Delta extension is available
INSTALL delta;
LOAD delta;

-- Create bronze schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS bronze;

-- Customers
CREATE OR REPLACE VIEW bronze.customers AS
SELECT * FROM delta_scan('C:\Users\sraikar\Documents\projects\APAC-DIA-Training-DE\lake\bronze\delta\customers\samples');

 --Orders_header
CREATE OR REPLACE VIEW bronze.orders_header AS
SELECT * FROM delta_scan('C:\Users\sraikar\Documents\projects\APAC-DIA-Training-DE\lake\bronze\delta\orders_header\samples');

-- Order Lines
CREATE OR REPLACE VIEW bronze.orders_lines AS
SELECT * FROM delta_scan('C:\Users\sraikar\Documents\projects\APAC-DIA-Training-DE\lake\bronze\delta\orders_lines\samples');

-- Products
CREATE OR REPLACE VIEW bronze.products AS
SELECT * FROM delta_scan('C:\Users\sraikar\Documents\projects\APAC-DIA-Training-DE\lake\bronze\delta\products\samples');

-- Stores
CREATE OR REPLACE VIEW bronze.stores AS
SELECT * FROM delta_scan('C:\Users\sraikar\Documents\projects\APAC-DIA-Training-DE\lake\bronze\delta\stores\samples');

-- Suppliers
CREATE OR REPLACE VIEW bronze.suppliers AS
SELECT * FROM delta_scan('C:\Users\sraikar\Documents\projects\APAC-DIA-Training-DE\lake\bronze\delta\suppliers\samples');

-- Returns
CREATE OR REPLACE VIEW bronze.returns AS
SELECT * FROM delta_scan('C:\Users\sraikar\Documents\projects\APAC-DIA-Training-DE\lake\bronze\delta\returns\samples');

-- Events
CREATE OR REPLACE VIEW bronze.events AS
SELECT * FROM delta_scan('C:\Users\sraikar\Documents\projects\APAC-DIA-Training-DE\lake\bronze\delta\events\samples');

-- Sensors
CREATE OR REPLACE VIEW bronze.sensors AS
SELECT * FROM delta_scan('C:\Users\sraikar\Documents\projects\APAC-DIA-Training-DE\lake\bronze\delta\sensors\samples');

-- Exchange Rates
CREATE OR REPLACE VIEW bronze.exchange_rates AS
SELECT * FROM delta_scan('C:\Users\sraikar\Documents\projects\APAC-DIA-Training-DE\lake\bronze\delta\exchange_rates\samples');

-- Shipments
CREATE OR REPLACE VIEW bronze.shipments AS
SELECT * FROM delta_scan('C:\Users\sraikar\Documents\projects\APAC-DIA-Training-DE\lake\bronze\delta\shipments\samples');


-- Ingestion Audit --? 



NOTE: Used Delta for all tables — simplifies dbt sources (all from one format), supports schema evolution, avoids having to decide case-by-case