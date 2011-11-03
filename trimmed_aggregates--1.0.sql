/* a custom data type serving as a state for the trimmed aggregate functions */
CREATE TYPE trimmed_agg_state AS (
    vals        numeric[],
    low_cut     float,
    high_cut    float
);

/* appends data to the array and sets the low/high cut */
CREATE OR REPLACE FUNCTION trimmed_append(p_state trimmed_agg_state, p_element numeric, p_low float, p_high float) RETURNS trimmed_agg_state
AS $$
BEGIN
    p_state.vals := array_append(p_state.vals, p_element);
    p_state.low_cut := p_low;
    p_state.high_cut := p_high;
    RETURN p_state;
END
$$ LANGUAGE plpgsql;

/* appends data to the state, when there is a single cut value */
CREATE OR REPLACE FUNCTION trimmed_append(p_state trimmed_agg_state, p_element numeric, p_cut float) RETURNS trimmed_agg_state
AS $$
BEGIN
    p_state.vals := array_append(p_state.vals, p_element);
    p_state.low_cut := p_cut;
    p_state.high_cut := p_cut;
    RETURN p_state;
END
$$ LANGUAGE plpgsql;

/* processes the aggregate state to compute average (at the end of aggregate evaluation) */
CREATE OR REPLACE FUNCTION avg_trimmed_final(p_state trimmed_agg_state) RETURNS numeric
AS $$
DECLARE
    v_result    numeric := 0;
    v_tmp       numeric[];
    v_length    int;
    v_from      int;
    v_to        int;
    v_cnt       int := 0;
BEGIN

    SELECT ARRAY(
        SELECT p_state.vals[s.i] AS "foo"
        FROM
            generate_series(array_lower(p_state.vals,1), array_upper(p_state.vals,1)) AS s(i)
        ORDER BY foo
    ) INTO v_tmp;

    v_length := array_length(p_state.vals, 1);

    v_from := 1 + floor(v_length*p_state.low_cut);
    v_to   := v_length - floor(v_length*p_state.high_cut);

    IF (v_from > v_to) THEN
        RETURN NULL;
    END IF;

    FOR v_idx IN v_from..v_to LOOP
        IF (v_tmp[v_idx] IS NOT NULL) THEN
            v_result := v_result + v_tmp[v_idx];
            v_cnt := v_cnt + 1;
        END IF;
    END LOOP;

    IF (v_cnt > 0) THEN
        RETURN v_result/v_cnt;
    ELSE
        RETURN 0;
    END IF;

END
$$ LANGUAGE plpgsql;

/* processes the aggregate state to compute variance (at the end of aggregate evaluation) */
CREATE OR REPLACE FUNCTION var_trimmed_final(p_state trimmed_agg_state) RETURNS numeric
AS $$
DECLARE
    v_result    numeric := 0;
    v_average   numeric := 0;
    v_tmp       numeric[];
    v_length    int;
    v_from      int;
    v_to        int;
    v_cnt       int := 0;
    v_sum_x     numeric := 0;
    v_sum_x2    numeric := 0;
BEGIN

    SELECT ARRAY(
        SELECT p_state.vals[s.i] AS "foo"
        FROM
            generate_series(array_lower(p_state.vals,1), array_upper(p_state.vals,1)) AS s(i)
        ORDER BY foo
    ) INTO v_tmp;

    v_length := array_length(p_state.vals, 1);

    v_from := 1 + floor(v_length*p_state.low_cut);
    v_to   := v_length - floor(v_length*p_state.high_cut);

    IF (v_from > v_to) THEN
        RETURN NULL;
    END IF;

    FOR v_idx IN v_from..v_to LOOP
        IF (v_tmp[v_idx] IS NOT NULL) THEN
            v_average := v_average + v_tmp[v_idx];
            v_cnt := v_cnt + 1;
        END IF;
    END LOOP;

    v_average := v_average / v_cnt;

    FOR v_idx IN v_from..v_to LOOP
        IF (v_tmp[v_idx] IS NOT NULL) THEN
            v_result := v_result + (v_tmp[v_idx] - v_average) * (v_tmp[v_idx] - v_average);
        END IF;
    END LOOP;

    IF (v_cnt > 0) THEN
        RETURN v_result/v_cnt;
    ELSE
        RETURN 0;
    END IF;

END
$$ LANGUAGE plpgsql;

/* processes the aggregate state to compute variance (at the end of aggregate evaluation) */
CREATE OR REPLACE FUNCTION var_pop_trimmed_final(p_state trimmed_agg_state) RETURNS numeric
AS $$
DECLARE
    v_result    numeric := 0;
    v_average   numeric := 0;
    v_tmp       numeric[];
    v_length    int;
    v_from      int;
    v_to        int;
    v_cnt       int := 0;
    v_sum_x     numeric := 0;
    v_sum_x2    numeric := 0;
BEGIN

    SELECT ARRAY(
        SELECT p_state.vals[s.i] AS "foo"
        FROM
            generate_series(array_lower(p_state.vals,1), array_upper(p_state.vals,1)) AS s(i)
        ORDER BY foo
    ) INTO v_tmp;

    v_length := array_length(p_state.vals, 1);

    v_from := 1 + floor(v_length*p_state.low_cut);
    v_to   := v_length - floor(v_length*p_state.high_cut);

    IF (v_from > v_to) THEN
        RETURN NULL;
    END IF;

    FOR v_idx IN v_from..v_to LOOP
        IF (v_tmp[v_idx] IS NOT NULL) THEN
            v_cnt := v_cnt + 1;
            v_sum_x := v_sum_x + v_tmp[v_idx];
            v_sum_x2 := v_sum_x2 + v_tmp[v_idx] * v_tmp[v_idx];
        END IF;
    END LOOP;

    IF (v_cnt > 0) THEN
        RETURN (v_cnt * v_sum_x2 - v_sum_x * v_sum_x) / (v_cnt * v_cnt);
    ELSE
        RETURN 0;
    END IF;

END
$$ LANGUAGE plpgsql;

/* processes the aggregate state to compute variance (at the end of aggregate evaluation) */
CREATE OR REPLACE FUNCTION var_samp_trimmed_final(p_state trimmed_agg_state) RETURNS numeric
AS $$
DECLARE
    v_result    numeric := 0;
    v_average   numeric := 0;
    v_tmp       numeric[];
    v_length    int;
    v_from      int;
    v_to        int;
    v_cnt       int := 0;
    v_sum_x     numeric := 0;
    v_sum_x2    numeric := 0;
BEGIN

    SELECT ARRAY(
        SELECT p_state.vals[s.i] AS "foo"
        FROM
            generate_series(array_lower(p_state.vals,1), array_upper(p_state.vals,1)) AS s(i)
        ORDER BY foo
    ) INTO v_tmp;

    v_length := array_length(p_state.vals, 1);

    v_from := 1 + floor(v_length*p_state.low_cut);
    v_to   := v_length - floor(v_length*p_state.high_cut);

    IF (v_from > v_to) THEN
        RETURN NULL;
    END IF;

    FOR v_idx IN v_from..v_to LOOP
        IF (v_tmp[v_idx] IS NOT NULL) THEN
            v_cnt := v_cnt + 1;
            v_sum_x := v_sum_x + v_tmp[v_idx];
            v_sum_x2 := v_sum_x2 + v_tmp[v_idx] * v_tmp[v_idx];
        END IF;
    END LOOP;

    IF (v_cnt > 0) THEN
        RETURN (v_cnt * v_sum_x2 - v_sum_x * v_sum_x) / (v_cnt * (v_cnt - 1));
    ELSE
        RETURN 0;
    END IF;

END
$$ LANGUAGE plpgsql;

/* processes the aggregate state to compute standard deviation (at the end of aggregate evaluation) */
CREATE OR REPLACE FUNCTION stddev_trimmed_final(p_state trimmed_agg_state) RETURNS numeric
AS $$
DECLARE
    v_result    numeric := 0;
    v_average   numeric := 0;
    v_tmp       numeric[];
    v_length    int;
    v_from      int;
    v_to        int;
    v_cnt       int := 0;
BEGIN

    SELECT ARRAY(
        SELECT p_state.vals[s.i] AS "foo"
        FROM
            generate_series(array_lower(p_state.vals,1), array_upper(p_state.vals,1)) AS s(i)
        ORDER BY foo
    ) INTO v_tmp;

    v_length := array_length(p_state.vals, 1);

    v_from := 1 + floor(v_length*p_state.low_cut);
    v_to   := v_length - floor(v_length*p_state.high_cut);

    IF (v_from > v_to) THEN
        RETURN NULL;
    END IF;

    FOR v_idx IN v_from..v_to LOOP
        IF (v_tmp[v_idx] IS NOT NULL) THEN
            v_average := v_average + v_tmp[v_idx];
            v_cnt := v_cnt + 1;
        END IF;
    END LOOP;

    v_average := v_average / v_cnt;

    FOR v_idx IN v_from..v_to LOOP
        IF (v_tmp[v_idx] IS NOT NULL) THEN
            v_result := v_result + (v_tmp[v_idx] - v_average) * (v_tmp[v_idx] - v_average);
        END IF;
    END LOOP;

    IF (v_cnt > 0) THEN
        RETURN sqrt(v_result/v_cnt);
    ELSE
        RETURN 0;
    END IF;

END
$$ LANGUAGE plpgsql;

/* processes the aggregate state to compute variance (at the end of aggregate evaluation) */
CREATE OR REPLACE FUNCTION stddev_pop_trimmed_final(p_state trimmed_agg_state) RETURNS numeric
AS $$
DECLARE
    v_result    numeric := 0;
    v_average   numeric := 0;
    v_tmp       numeric[];
    v_length    int;
    v_from      int;
    v_to        int;
    v_cnt       int := 0;
    v_sum_x     numeric := 0;
    v_sum_x2    numeric := 0;
BEGIN

    SELECT ARRAY(
        SELECT p_state.vals[s.i] AS "foo"
        FROM
            generate_series(array_lower(p_state.vals,1), array_upper(p_state.vals,1)) AS s(i)
        ORDER BY foo
    ) INTO v_tmp;

    v_length := array_length(p_state.vals, 1);

    v_from := 1 + floor(v_length*p_state.low_cut);
    v_to   := v_length - floor(v_length*p_state.high_cut);

    IF (v_from > v_to) THEN
        RETURN NULL;
    END IF;

    FOR v_idx IN v_from..v_to LOOP
        IF (v_tmp[v_idx] IS NOT NULL) THEN
            v_cnt := v_cnt + 1;
            v_sum_x := v_sum_x + v_tmp[v_idx];
            v_sum_x2 := v_sum_x2 + v_tmp[v_idx] * v_tmp[v_idx];
        END IF;
    END LOOP;

    IF (v_cnt > 0) THEN
        RETURN sqrt((v_cnt * v_sum_x2 - v_sum_x * v_sum_x) / (v_cnt * v_cnt));
    ELSE
        RETURN 0;
    END IF;

END
$$ LANGUAGE plpgsql;

/* processes the aggregate state to compute variance (at the end of aggregate evaluation) */
CREATE OR REPLACE FUNCTION stddev_samp_trimmed_final(p_state trimmed_agg_state) RETURNS numeric
AS $$
DECLARE
    v_result    numeric := 0;
    v_average   numeric := 0;
    v_tmp       numeric[];
    v_length    int;
    v_from      int;
    v_to        int;
    v_cnt       int := 0;
    v_sum_x     numeric := 0;
    v_sum_x2    numeric := 0;
BEGIN

    SELECT ARRAY(
        SELECT p_state.vals[s.i] AS "foo"
        FROM
            generate_series(array_lower(p_state.vals,1), array_upper(p_state.vals,1)) AS s(i)
        ORDER BY foo
    ) INTO v_tmp;

    v_length := array_length(p_state.vals, 1);

    v_from := 1 + floor(v_length*p_state.low_cut);
    v_to   := v_length - floor(v_length*p_state.high_cut);

    IF (v_from > v_to) THEN
        RETURN NULL;
    END IF;

    FOR v_idx IN v_from..v_to LOOP
        IF (v_tmp[v_idx] IS NOT NULL) THEN
            v_cnt := v_cnt + 1;
            v_sum_x := v_sum_x + v_tmp[v_idx];
            v_sum_x2 := v_sum_x2 + v_tmp[v_idx] * v_tmp[v_idx];
        END IF;
    END LOOP;

    IF (v_cnt > 0) THEN
        RETURN sqrt((v_cnt * v_sum_x2 - v_sum_x * v_sum_x) / (v_cnt * (v_cnt - 1)));
    ELSE
        RETURN 0;
    END IF;

END
$$ LANGUAGE plpgsql;


/* trimmed average */
CREATE AGGREGATE avg_trimmed(numeric, float, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = avg_trimmed_final
);

/* trimmed average */
CREATE AGGREGATE avg_trimmed(numeric, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = avg_trimmed_final
);

/* trimmed variance */
CREATE AGGREGATE var_trimmed(numeric, float, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = var_trimmed_final
);

/* trimmed variance */
CREATE AGGREGATE var_trimmed(numeric, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = var_trimmed_final
);

/* trimmed variance (population) */
CREATE AGGREGATE var_pop_trimmed(numeric, float, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = var_pop_trimmed_final
);

/* trimmed variance (population) */
CREATE AGGREGATE var_pop_trimmed(numeric, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = var_pop_trimmed_final
);

/* trimmed variance (sample) */
CREATE AGGREGATE var_samp_trimmed(numeric, float, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = var_samp_trimmed_final
);

/* trimmed variance (sample) */
CREATE AGGREGATE var_samp_trimmed(numeric, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = var_samp_trimmed_final
);

/* trimmed standard deviation */
CREATE AGGREGATE stddev_trimmed(numeric, float, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = stddev_trimmed_final
);

/* trimmed standard deviation */
CREATE AGGREGATE stddev_trimmed(numeric, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = stddev_trimmed_final
);

/* trimmed standard deviation (population) */
CREATE AGGREGATE stddev_pop_trimmed(numeric, float, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = stddev_pop_trimmed_final
);

/* trimmed standard deviation (population) */
CREATE AGGREGATE stddev_pop_trimmed(numeric, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = stddev_pop_trimmed_final
);

/* trimmed standard deviation (sample) */
CREATE AGGREGATE stddev_samp_trimmed(numeric, float, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = stddev_samp_trimmed_final
);

/* trimmed standard deviation (sample) */
CREATE AGGREGATE stddev_samp_trimmed(numeric, float) (
    SFUNC = trimmed_append,
    STYPE = trimmed_agg_state,
    FINALFUNC = stddev_samp_trimmed_final
);
