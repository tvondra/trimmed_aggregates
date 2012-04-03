BEGIN;
CREATE EXTENSION trimmed_aggregates;

-- the regression tests round the values a bit so that rounding errors don't trigger failures
-- wrapper to handle rounding for double precision values
CREATE OR REPLACE FUNCTION round(p_val double precision, p_digits int) RETURNS double precision AS $$
    SELECT round($1 * pow(10,$2)) / pow(10,$2);
$$ LANGUAGE sql;

-- int
SELECT round(avg_trimmed(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_trimmed(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_pop_trimmed(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_samp_trimmed(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_trimmed(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_pop_trimmed(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_samp_trimmed(x, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);

-- bigint
SELECT round(avg_trimmed(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_trimmed(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_pop_trimmed(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_samp_trimmed(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_trimmed(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_pop_trimmed(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_samp_trimmed(x::bigint, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);

-- double precision
SELECT round(avg_trimmed(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_trimmed(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_pop_trimmed(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_samp_trimmed(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_trimmed(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_pop_trimmed(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_samp_trimmed(x::double precision, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);

-- numeric
SELECT round(avg_trimmed(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_trimmed(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_pop_trimmed(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(var_samp_trimmed(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_trimmed(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_pop_trimmed(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);
SELECT round(stddev_samp_trimmed(x::numeric, 0.1, 0.1),3) FROM generate_series(1,1000) s(x);

ROLLBACK;