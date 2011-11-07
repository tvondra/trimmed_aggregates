MODULE_big = trimmed_aggregates
OBJS = src/trimmed.o

EXTENSION = trimmed_aggregates
DATA = sql/trimmed_aggregates--1.0.sql
MODULES = trimmed_aggregates

CFLAGS=`pg_config --includedir-server`

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

trimmed_aggregates.so: src/trimmed.o

src/trimmed.o: src/trimmed.c