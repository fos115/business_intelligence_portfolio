-- 1. Crear una base de datos y schema
CREATE DATABASE ecommerce_db;
USE DATABASE ecommerce_db;
CREATE SCHEMA sales;

-- 2. Crear tablas
CREATE OR REPLACE TABLE sales.customers (
    customer_id INT,
    name STRING,
    email STRING,
    registration_date DATE
);

CREATE OR REPLACE TABLE sales.products (
    product_id INT,
    name STRING,
    category STRING,
    price DECIMAL(10,2)
);

CREATE OR REPLACE TABLE sales.orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2)
);

CREATE OR REPLACE TABLE sales.order_items (
    order_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10,2)
);

-- 3. Cargar datos de ejemplo (asumiendo que tienes archivos CSV en un stage)
COPY INTO sales.customers FROM @my_csv_stage/customers.csv FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
COPY INTO sales.products FROM @my_csv_stage/products.csv FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
COPY INTO sales.orders FROM @my_csv_stage/orders.csv FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
COPY INTO sales.order_items FROM @my_csv_stage/order_items.csv FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- 4. Crear una vista para análisis de ventas
CREATE OR REPLACE VIEW sales.sales_analysis AS
SELECT 
    o.order_id,
    o.order_date,
    c.name AS customer_name,
    p.name AS product_name,
    p.category,
    oi.quantity,
    oi.price AS unit_price,
    (oi.quantity * oi.price) AS total_price
FROM sales.orders o
JOIN sales.customers c ON o.customer_id = c.customer_id
JOIN sales.order_items oi ON o.order_id = oi.order_id
JOIN sales.products p ON oi.product_id = p.product_id;

-- 5. Crear una tabla temporal para almacenar datos agregados
CREATE OR REPLACE TEMPORARY TABLE sales.daily_sales AS
SELECT 
    order_date,
    SUM(total_price) AS daily_revenue,
    COUNT(DISTINCT order_id) AS num_orders
FROM sales.sales_analysis
GROUP BY order_date;

-- 6. Crear una tarea programada para actualizar los datos diarios
CREATE OR REPLACE TASK update_daily_sales
WAREHOUSE = compute_wh
SCHEDULE = 'USING CRON 0 1 * * * America/New_York'
AS
MERGE INTO sales.daily_sales t
USING (
    SELECT 
        order_date,
        SUM(total_price) AS daily_revenue,
        COUNT(DISTINCT order_id) AS num_orders
    FROM sales.sales_analysis
    WHERE order_date = CURRENT_DATE() - 1
    GROUP BY order_date
) s
ON t.order_date = s.order_date
WHEN MATCHED THEN UPDATE SET
    t.daily_revenue = s.daily_revenue,
    t.num_orders = s.num_orders
WHEN NOT MATCHED THEN INSERT
    (order_date, daily_revenue, num_orders)
VALUES
    (s.order_date, s.daily_revenue, s.num_orders);

-- 7. Crear una función definida por el usuario para calcular el valor del cliente
CREATE OR REPLACE FUNCTION sales.calculate_customer_value(customer_id INT)
RETURNS FLOAT
AS
$$
    SELECT SUM(total_amount)
    FROM sales.orders
    WHERE customer_id = customer_id
$$;

-- 8. Ejemplo de consulta para análisis de BI
SELECT 
    category,
    SUM(total_price) AS revenue,
    COUNT(DISTINCT order_id) AS num_orders,
    SUM(total_price) / COUNT(DISTINCT order_id) AS avg_order_value
FROM sales.sales_analysis
WHERE order_date BETWEEN DATEADD(month, -1, CURRENT_DATE()) AND CURRENT_DATE()
GROUP BY category
ORDER BY revenue DESC;
