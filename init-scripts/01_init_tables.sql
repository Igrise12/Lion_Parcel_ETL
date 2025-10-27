-- Create tables
CREATE TABLE IF NOT EXISTS posts (
    post_id INT PRIMARY KEY,
    post_origin VARCHAR(100),
    post_destination VARCHAR(100),
    created_at DATETIME,
    updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    customer_email VARCHAR(100),
    customer_phone VARCHAR(50),
    created_at DATETIME,
    updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS transactions (
    id INT PRIMARY KEY,
    customer_id INT,
    post_id INT,
    last_status VARCHAR(20),
    created_at DATETIME,
    updated_at DATETIME,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (post_id) REFERENCES posts(post_id)
);