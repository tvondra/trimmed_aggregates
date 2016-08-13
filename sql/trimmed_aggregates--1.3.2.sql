/* accumulating data */
CREATE OR REPLACE FUNCTION trimmed_append_double(p_pointer internal, p_element double precision, p_cut_low double precision default 0, p_cut_up double precision default 0)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_append_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_append_int32(p_pointer internal, p_element int, p_cut_low double precision default 0, p_cut_up double precision default 0)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_append_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_append_int64(p_pointer internal, p_element bigint, p_cut_low double precision default 0, p_cut_up double precision default 0)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_append_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_append_numeric(p_pointer internal, p_element numeric, p_cut_low double precision default 0, p_cut_up double precision default 0)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_append_numeric'
    LANGUAGE C IMMUTABLE;

/* average */
CREATE OR REPLACE FUNCTION trimmed_avg_double(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_avg_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_avg_int32(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_avg_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_avg_int64(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_avg_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_avg_numeric(p_pointer internal)
    RETURNS numeric
    AS 'trimmed_aggregates', 'trimmed_avg_numeric'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE avg_trimmed(double precision, double precision, double precision) (
    SFUNC = trimmed_append_double,
    STYPE = internal,
    FINALFUNC = trimmed_avg_double
);

CREATE AGGREGATE avg_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_avg_int32
);

CREATE AGGREGATE avg_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_avg_int64
);

CREATE AGGREGATE avg_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_avg_numeric
);

/* variance */
CREATE OR REPLACE FUNCTION trimmed_var_double(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_var_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_var_int32(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_var_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_var_int64(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_var_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_var_numeric(p_pointer internal)
    RETURNS numeric
    AS 'trimmed_aggregates', 'trimmed_var_numeric'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE var_trimmed(double precision, double precision, double precision) (
    SFUNC = trimmed_append_double,
    STYPE = internal,
    FINALFUNC = trimmed_var_double
);

CREATE AGGREGATE var_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_var_int32
);

CREATE AGGREGATE var_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_var_int64
);

CREATE AGGREGATE var_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_var_numeric
);

/* variance (population estimate) */
CREATE OR REPLACE FUNCTION trimmed_var_pop_double(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_var_pop_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_var_pop_int32(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_var_pop_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_var_pop_int64(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_var_pop_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_var_pop_numeric(p_pointer internal)
    RETURNS numeric
    AS 'trimmed_aggregates', 'trimmed_var_pop_numeric'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE var_pop_trimmed(double precision, double precision, double precision) (
    SFUNC = trimmed_append_double,
    STYPE = internal,
    FINALFUNC = trimmed_var_pop_double
);

CREATE AGGREGATE var_pop_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_var_pop_int32
);

CREATE AGGREGATE var_pop_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_var_pop_int64
);

CREATE AGGREGATE var_pop_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_var_pop_numeric
);

/* variance (sample estimate) */
CREATE OR REPLACE FUNCTION trimmed_var_samp_double(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_var_samp_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_var_samp_int32(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_var_samp_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_var_samp_int64(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_var_samp_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_var_samp_numeric(p_pointer internal)
    RETURNS numeric
    AS 'trimmed_aggregates', 'trimmed_var_samp_numeric'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE var_samp_trimmed(double precision, double precision, double precision) (
    SFUNC = trimmed_append_double,
    STYPE = internal,
    FINALFUNC = trimmed_var_samp_double
);

CREATE AGGREGATE var_samp_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_var_samp_int32
);

CREATE AGGREGATE var_samp_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_var_samp_int64
);

CREATE AGGREGATE var_samp_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_var_samp_numeric
);

/* variance */
CREATE OR REPLACE FUNCTION trimmed_stddev_double(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_stddev_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_stddev_int32(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_stddev_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_stddev_int64(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_stddev_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_stddev_numeric(p_pointer internal)
    RETURNS numeric
    AS 'trimmed_aggregates', 'trimmed_stddev_numeric'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE stddev_trimmed(double precision, double precision, double precision) (
    SFUNC = trimmed_append_double,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_double
);

CREATE AGGREGATE stddev_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_int32
);

CREATE AGGREGATE stddev_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_int64
);

CREATE AGGREGATE stddev_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_numeric
);

/* variance (population estimate) */
CREATE OR REPLACE FUNCTION trimmed_stddev_pop_double(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_stddev_pop_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_stddev_pop_int32(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_stddev_pop_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_stddev_pop_int64(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_stddev_pop_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_stddev_pop_numeric(p_pointer internal)
    RETURNS numeric
    AS 'trimmed_aggregates', 'trimmed_stddev_pop_numeric'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE stddev_pop_trimmed(double precision, double precision, double precision) (
    SFUNC = trimmed_append_double,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_pop_double
);

CREATE AGGREGATE stddev_pop_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_pop_int32
);

CREATE AGGREGATE stddev_pop_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_pop_int64
);

CREATE AGGREGATE stddev_pop_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_pop_numeric
);

/* variance (sample estimate) */
CREATE OR REPLACE FUNCTION trimmed_stddev_samp_double(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_stddev_samp_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_stddev_samp_int32(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_stddev_samp_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_stddev_samp_int64(p_pointer internal)
    RETURNS double precision
    AS 'trimmed_aggregates', 'trimmed_stddev_samp_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_stddev_samp_numeric(p_pointer internal)
    RETURNS numeric
    AS 'trimmed_aggregates', 'trimmed_stddev_samp_numeric'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE stddev_samp_trimmed(double precision, double precision, double precision) (
    SFUNC = trimmed_append_double,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_samp_double
);

CREATE AGGREGATE stddev_samp_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_samp_int32
);

CREATE AGGREGATE stddev_samp_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_samp_int64
);

CREATE AGGREGATE stddev_samp_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_samp_numeric
);

/* aggregate producing complete result (average, variances etc.) */

CREATE OR REPLACE FUNCTION trimmed_double_array(p_pointer internal)
    RETURNS double precision[]
    AS 'trimmed_aggregates', 'trimmed_double_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_int32_array(p_pointer internal)
    RETURNS double precision[]
    AS 'trimmed_aggregates', 'trimmed_int32_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_int64_array(p_pointer internal)
    RETURNS double precision[]
    AS 'trimmed_aggregates', 'trimmed_int64_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_numeric_array(p_pointer internal)
    RETURNS numeric[]
    AS 'trimmed_aggregates', 'trimmed_numeric_array'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE trimmed(double precision, double precision, double precision) (
    SFUNC = trimmed_append_double,
    STYPE = internal,
    FINALFUNC = trimmed_double_array
);

CREATE AGGREGATE trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_int32_array
);

CREATE AGGREGATE trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_int64_array
);

CREATE AGGREGATE trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_numeric_array
);
