--
-- Run this script as the 'postgres' user!
-- Create the pgdb_tools database and user.
-- 

DROP DATABASE IF EXISTS pgdb_tools;
DROP USER IF EXISTS pgdb_tools;

CREATE DATABASE pgdb_tools
       ENCODING 'UTF8'
       TEMPLATE template0;

CREATE USER pgdb_tools PASSWORD 'password';
GRANT ALL ON DATABASE pgdb_tools TO pgdb_tools;

--
-- Create template tables using the 'pgdb_tools' user
--
\connect pgdb_tools;

SET ROLE pgdb_tools;


CREATE TABLE pgdb_tools_test (
    int INTEGER,
    day DATE,
    message TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

INSERT INTO pgdb_tools_test (int, day, message)
VALUES
(1, '2013-01-01', 'One'),
(2, '2013-01-02', 'Two'),
(3, '2013-01-03', 'Three');


