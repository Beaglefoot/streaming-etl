# staging-db

`*_buffer` tables are temporary which get filled and cleaned by transformer.

This allows transformer to continue its work on after crash.

`*_lookup` tables are never cleaned and with emergence of new data sources can include another column to map domain-specific ID to DWH key.
