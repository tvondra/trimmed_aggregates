MODULE_big = trimmed_aggregates
OBJS = src/trimmed.o

EXTENSION = trimmed_aggregates
DATA =  sql/trimmed_aggregates--2.0.0.sql sql/trimmed_aggregates--1.3.1--1.3.2.sql sql/trimmed_aggregates--1.3.2--2.0.0.sql
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
