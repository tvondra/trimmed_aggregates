EXTENSION = trimmed_aggregates
DATA = trimmed_aggregates--1.0.sql

CFLAGS=`pg_config --includedir-server`

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
