EXTENSION = pg_flexflake
DATA = pg_flexflake--1.0.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)