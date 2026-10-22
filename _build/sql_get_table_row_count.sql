select table_name, table_rows
from information_schema.tables
where table_catalog = 'def'
and table_schema = 't16o_db'
and table_type = 'BASE TABLE'
order by 2 desc
;

CALL `t16o_db`.`sp_tx_clear_tables`();
