\set ECHO none
BEGIN;

-- disable the notices for the create script (shell types etc.)
SET client_min_messages = 'WARNING';
\i sql/trimmed_aggregates--2.0.0-dev.sql
SET client_min_messages = 'NOTICE';

\set ECHO all

-- the regression tests round the values a bit so that rounding errors don't trigger failures
-- wrapper to handle rounding for double precision values
CREATE OR REPLACE FUNCTION round(p_val double precision, p_digits int) RETURNS double precision AS $$
    SELECT round($1 * pow(10,$2)) / pow(10,$2);
$$ LANGUAGE sql;

-- int
SELECT round(avg(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_pop(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_samp(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_pop(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_samp(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);

-- bigint
SELECT round(avg(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_pop(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_samp(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_pop(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_samp(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);

-- double precision
SELECT round(avg(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_pop(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_samp(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_pop(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_samp(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);

-- numeric
SELECT round(avg(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_pop(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_samp(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_pop(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_samp(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);

ROLLBACK;
