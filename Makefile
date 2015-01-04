MODULE_big = trimmed_aggregates
OBJS = src/trimmed.o

EXTENSION = trimmed_aggregates
DATA = sql/trimmed_aggregates--1.0.sql sql/trimmed_aggregates--1.1.sql sql/trimmed_aggregates--1.2.sql sql/trimmed_aggregates--1.2.2.sql sql/trimmed_aggregates--1.3.0.sql sql/trimmed_aggregates--1.3.1.sql
MODULES = trimmed_aggregates

CFLAGS=`pg_config --includedir-server`

TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

trimmed_aggregates.so: src/trimmed.o

src/trimmed.o: src/trimmed.c
