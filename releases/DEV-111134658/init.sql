postgres=# grant all on schema public to "srv.etl.dwh";
GRANT



dwh=# create schema webapp;
CREATE SCHEMA
dwh=# grant all on schema webapp to "srv.etl.dwh";
GRANT
dwh=# grant all on schema webapp to mseleznev;