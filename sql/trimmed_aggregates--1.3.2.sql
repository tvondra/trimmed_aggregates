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

/* combine data */
CREATE OR REPLACE FUNCTION trimmed_combine_double(p_state_1 internal, p_state_2 internal)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_combine_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_combine_int32(p_state_1 internal, p_state_2 internal)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_combine_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_combine_int64(p_state_1 internal, p_state_2 internal)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_combine_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_combine_numeric(p_state_1 internal, p_state_2 internal)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_combine_numeric'
    LANGUAGE C IMMUTABLE;

/* serialize data */
CREATE OR REPLACE FUNCTION trimmed_serial_double(p_pointer internal)
    RETURNS bytea
    AS 'trimmed_aggregates', 'trimmed_serial_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_serial_int32(p_pointer internal)
    RETURNS bytea
    AS 'trimmed_aggregates', 'trimmed_serial_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_serial_int64(p_pointer internal)
    RETURNS bytea
    AS 'trimmed_aggregates', 'trimmed_serial_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_serial_numeric(p_pointer internal)
    RETURNS bytea
    AS 'trimmed_aggregates', 'trimmed_serial_numeric'
    LANGUAGE C IMMUTABLE;

/* deserialize data */
CREATE OR REPLACE FUNCTION trimmed_deserial_double(p_value bytea, p_dummy internal)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_deserial_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_deserial_int32(p_value bytea, p_dummy internal)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_deserial_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_deserial_int64(p_value bytea, p_dummy internal)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_deserial_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION trimmed_deserial_numeric(p_value bytea, p_dummy internal)
    RETURNS internal
    AS 'trimmed_aggregates', 'trimmed_deserial_numeric'
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
    FINALFUNC = trimmed_avg_double,
    COMBINEFUNC = trimmed_combine_double,
    SERIALFUNC = trimmed_serial_double,
    DESERIALFUNC = trimmed_deserial_double,
    PARALLEL = SAFE
);

CREATE AGGREGATE avg_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_avg_int32,
    COMBINEFUNC = trimmed_combine_int32,
    SERIALFUNC = trimmed_serial_int32,
    DESERIALFUNC = trimmed_deserial_int32,
    PARALLEL = SAFE
);

CREATE AGGREGATE avg_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_avg_int64,
    COMBINEFUNC = trimmed_combine_int64,
    SERIALFUNC = trimmed_serial_int64,
    DESERIALFUNC = trimmed_deserial_int64,
    PARALLEL = SAFE
);

CREATE AGGREGATE avg_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_avg_numeric,
    COMBINEFUNC = trimmed_combine_numeric,
    SERIALFUNC = trimmed_serial_numeric,
    DESERIALFUNC = trimmed_deserial_numeric,
    PARALLEL = SAFE
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
    FINALFUNC = trimmed_var_double,
    COMBINEFUNC = trimmed_combine_double,
    SERIALFUNC = trimmed_serial_double,
    DESERIALFUNC = trimmed_deserial_double,
    PARALLEL = SAFE
);

CREATE AGGREGATE var_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_var_int32,
    COMBINEFUNC = trimmed_combine_int32,
    SERIALFUNC = trimmed_serial_int32,
    DESERIALFUNC = trimmed_deserial_int32,
    PARALLEL = SAFE
);

CREATE AGGREGATE var_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_var_int64,
    COMBINEFUNC = trimmed_combine_int64,
    SERIALFUNC = trimmed_serial_int64,
    DESERIALFUNC = trimmed_deserial_int64,
    PARALLEL = SAFE
);

CREATE AGGREGATE var_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_var_numeric,
    COMBINEFUNC = trimmed_combine_numeric,
    SERIALFUNC = trimmed_serial_numeric,
    DESERIALFUNC = trimmed_deserial_numeric,
    PARALLEL = SAFE
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
    FINALFUNC = trimmed_var_pop_double,
    COMBINEFUNC = trimmed_combine_double,
    SERIALFUNC = trimmed_serial_double,
    DESERIALFUNC = trimmed_deserial_double,
    PARALLEL = SAFE
);

CREATE AGGREGATE var_pop_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_var_pop_int32,
    COMBINEFUNC = trimmed_combine_int32,
    SERIALFUNC = trimmed_serial_int32,
    DESERIALFUNC = trimmed_deserial_int32,
    PARALLEL = SAFE
);

CREATE AGGREGATE var_pop_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_var_pop_int64,
    COMBINEFUNC = trimmed_combine_int64,
    SERIALFUNC = trimmed_serial_int64,
    DESERIALFUNC = trimmed_deserial_int64,
    PARALLEL = SAFE
);

CREATE AGGREGATE var_pop_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_var_pop_numeric,
    COMBINEFUNC = trimmed_combine_numeric,
    SERIALFUNC = trimmed_serial_numeric,
    DESERIALFUNC = trimmed_deserial_numeric,
    PARALLEL = SAFE
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
    FINALFUNC = trimmed_var_samp_double,
    COMBINEFUNC = trimmed_combine_double,
    SERIALFUNC = trimmed_serial_double,
    DESERIALFUNC = trimmed_deserial_double,
    PARALLEL = SAFE
);

CREATE AGGREGATE var_samp_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_var_samp_int32,
    COMBINEFUNC = trimmed_combine_int32,
    SERIALFUNC = trimmed_serial_int32,
    DESERIALFUNC = trimmed_deserial_int32,
    PARALLEL = SAFE
);

CREATE AGGREGATE var_samp_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_var_samp_int64,
    COMBINEFUNC = trimmed_combine_int64,
    SERIALFUNC = trimmed_serial_int64,
    DESERIALFUNC = trimmed_deserial_int64,
    PARALLEL = SAFE
);

CREATE AGGREGATE var_samp_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_var_samp_numeric,
    COMBINEFUNC = trimmed_combine_numeric,
    SERIALFUNC = trimmed_serial_numeric,
    DESERIALFUNC = trimmed_deserial_numeric,
    PARALLEL = SAFE
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
    FINALFUNC = trimmed_stddev_double,
    COMBINEFUNC = trimmed_combine_double,
    SERIALFUNC = trimmed_serial_double,
    DESERIALFUNC = trimmed_deserial_double,
    PARALLEL = SAFE
);

CREATE AGGREGATE stddev_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_int32,
    COMBINEFUNC = trimmed_combine_int32,
    SERIALFUNC = trimmed_serial_int32,
    DESERIALFUNC = trimmed_deserial_int32,
    PARALLEL = SAFE
);

CREATE AGGREGATE stddev_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_int64,
    COMBINEFUNC = trimmed_combine_int64,
    SERIALFUNC = trimmed_serial_int64,
    DESERIALFUNC = trimmed_deserial_int64,
    PARALLEL = SAFE
);

CREATE AGGREGATE stddev_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_numeric,
    COMBINEFUNC = trimmed_combine_numeric,
    SERIALFUNC = trimmed_serial_numeric,
    DESERIALFUNC = trimmed_deserial_numeric,
    PARALLEL = SAFE
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
    FINALFUNC = trimmed_stddev_pop_double,
    COMBINEFUNC = trimmed_combine_double,
    SERIALFUNC = trimmed_serial_double,
    DESERIALFUNC = trimmed_deserial_double,
    PARALLEL = SAFE
);

CREATE AGGREGATE stddev_pop_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_pop_int32,
    COMBINEFUNC = trimmed_combine_int32,
    SERIALFUNC = trimmed_serial_int32,
    DESERIALFUNC = trimmed_deserial_int32,
    PARALLEL = SAFE
);

CREATE AGGREGATE stddev_pop_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_pop_int64,
    COMBINEFUNC = trimmed_combine_int64,
    SERIALFUNC = trimmed_serial_int64,
    DESERIALFUNC = trimmed_deserial_int64,
    PARALLEL = SAFE
);

CREATE AGGREGATE stddev_pop_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_pop_numeric,
    COMBINEFUNC = trimmed_combine_numeric,
    SERIALFUNC = trimmed_serial_numeric,
    DESERIALFUNC = trimmed_deserial_numeric,
    PARALLEL = SAFE
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
    FINALFUNC = trimmed_stddev_samp_double,
    COMBINEFUNC = trimmed_combine_double,
    SERIALFUNC = trimmed_serial_double,
    DESERIALFUNC = trimmed_deserial_double,
    PARALLEL = SAFE
);

CREATE AGGREGATE stddev_samp_trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_samp_int32,
    COMBINEFUNC = trimmed_combine_int32,
    SERIALFUNC = trimmed_serial_int32,
    DESERIALFUNC = trimmed_deserial_int32,
    PARALLEL = SAFE
);

CREATE AGGREGATE stddev_samp_trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_samp_int64,
    COMBINEFUNC = trimmed_combine_int64,
    SERIALFUNC = trimmed_serial_int64,
    DESERIALFUNC = trimmed_deserial_int64,
    PARALLEL = SAFE
);

CREATE AGGREGATE stddev_samp_trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_stddev_samp_numeric,
    COMBINEFUNC = trimmed_combine_numeric,
    SERIALFUNC = trimmed_serial_numeric,
    DESERIALFUNC = trimmed_deserial_numeric,
    PARALLEL = SAFE
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
    FINALFUNC = trimmed_double_array,
    COMBINEFUNC = trimmed_combine_double,
    SERIALFUNC = trimmed_serial_double,
    DESERIALFUNC = trimmed_deserial_double,
    PARALLEL = SAFE
);

CREATE AGGREGATE trimmed(int, double precision, double precision) (
    SFUNC = trimmed_append_int32,
    STYPE = internal,
    FINALFUNC = trimmed_int32_array,
    COMBINEFUNC = trimmed_combine_int32,
    SERIALFUNC = trimmed_serial_int32,
    DESERIALFUNC = trimmed_deserial_int32,
    PARALLEL = SAFE
);

CREATE AGGREGATE trimmed(bigint, double precision, double precision) (
    SFUNC = trimmed_append_int64,
    STYPE = internal,
    FINALFUNC = trimmed_int64_array,
    COMBINEFUNC = trimmed_combine_int64,
    SERIALFUNC = trimmed_serial_int64,
    DESERIALFUNC = trimmed_deserial_int64,
    PARALLEL = SAFE
);

CREATE AGGREGATE trimmed(numeric, double precision, double precision) (
    SFUNC = trimmed_append_numeric,
    STYPE = internal,
    FINALFUNC = trimmed_numeric_array,
    COMBINEFUNC = trimmed_combine_numeric,
    SERIALFUNC = trimmed_serial_numeric,
    DESERIALFUNC = trimmed_deserial_numeric,
    PARALLEL = SAFE
);
