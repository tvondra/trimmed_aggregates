/*
 * trimmed.c - Trimmed aggregate functions
 * Copyright (C) Tomas Vondra, 2011-2016
 *
 *
 * Implementation of trimmed avg/stddev/var aggregates.
 *
 * The memory consumption might be a problem, as all the values are kept in
 * memory - for example 1.000.000 of 8-byte values (bigint) requires about
 * 8MB of memory.
 */

#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "postgres.h"
#include "utils/datum.h"
#include "utils/palloc.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "nodes/memnodes.h"
#include "fmgr.h"
#include "catalog/pg_type.h"

#include "funcapi.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


#define GET_AGG_CONTEXT(fname, fcinfo, aggcontext)  \
	if (! AggCheckCallContext(fcinfo, &aggcontext)) {   \
		elog(ERROR, "%s called in non-aggregate context", fname);  \
	}

#define CHECK_AGG_CONTEXT(fname, fcinfo)  \
	if (! AggCheckCallContext(fcinfo, NULL)) {   \
		elog(ERROR, "%s called in non-aggregate context", fname);  \
	}


/* how many elements to start with */
#define MIN_ELEMENTS	32

static Numeric const_zero = NULL;

/* FIXME The final functions copy a lot of code - refactor to share. */

/* Structures used to keep the data - the 'elements' array is extended
 * on the fly if needed. */

typedef struct state_double
{
	int		maxelements;	/* size of elements array */
	int		nelements;		/* number of used items */

	double	cut_lower;		/* fraction to cut at the lower end */
	double	cut_upper;		/* fraction to cut at the upper end */

	bool	sorted;			/* are the elements sorted */

	double *elements;		/* array of values */
} state_double;

typedef struct state_int32
{
	int		maxelements;	/* size of elements array */
	int		nelements;		/* number of used items */

	double	cut_lower;		/* fraction to cut at the lower end */
	double	cut_upper;		/* fraction to cut at the upper end */

	bool	sorted;			/* are the elements sorted */

	int32  *elements;		/* array of values */
} state_int32;

typedef struct state_int64
{
	int		maxelements;	/* size of elements array */
	int		nelements;		/* number of used items */

	double	cut_lower;		/* fraction to cut at the lower end */
	double	cut_upper;		/* fraction to cut at the upper end */

	bool	sorted;			/* are the elements sorted */

	int64  *elements;		/* array of values */
} state_int64;

typedef struct state_numeric
{
	int		nelements;		/* number of stored items */

	double	cut_lower;		/* fraction to cut at the lower end */
	double	cut_upper;		/* fraction to cut at the upper end */

	bool	sorted;			/* are the elements sorted */

	int		maxlen;			/* total size of the buffer */
	int		usedlen;		/* used part of the buffer */

	char    *data;			/* contents of the numeric values */
} state_numeric;

/* comparators, used for qsort */

static int  double_comparator(const void *a, const void *b);
static int  int32_comparator(const void *a, const void *b);
static int  int64_comparator(const void *a, const void *b);
static int  numeric_comparator(const void *a, const void *b);

static void sort_state_double(state_double *state);
static void sort_state_int32(state_int32 *state);
static void sort_state_int64(state_int64 *state);
static void sort_state_numeric(state_numeric *state);

static Numeric *numeric_zero(void);

static Datum
double_to_array(FunctionCallInfo fcinfo, double * d, int len);

static Datum
numeric_to_array(FunctionCallInfo fcinfo, Numeric * d, int len);

/* ACCUMULATE DATA */

PG_FUNCTION_INFO_V1(trimmed_append_double);
PG_FUNCTION_INFO_V1(trimmed_append_int32);
PG_FUNCTION_INFO_V1(trimmed_append_int64);
PG_FUNCTION_INFO_V1(trimmed_append_numeric);

Datum trimmed_append_double(PG_FUNCTION_ARGS);
Datum trimmed_append_int32(PG_FUNCTION_ARGS);
Datum trimmed_append_int64(PG_FUNCTION_ARGS);
Datum trimmed_append_numeric(PG_FUNCTION_ARGS);

/* AVERAGE */

PG_FUNCTION_INFO_V1(trimmed_avg_double);
PG_FUNCTION_INFO_V1(trimmed_avg_int32);
PG_FUNCTION_INFO_V1(trimmed_avg_int64);
PG_FUNCTION_INFO_V1(trimmed_avg_numeric);

Datum trimmed_avg_double(PG_FUNCTION_ARGS);
Datum trimmed_avg_int32(PG_FUNCTION_ARGS);
Datum trimmed_avg_int64(PG_FUNCTION_ARGS);
Datum trimmed_avg_numeric(PG_FUNCTION_ARGS);

/* VARIANCE */

/* exact */
PG_FUNCTION_INFO_V1(trimmed_var_double);
PG_FUNCTION_INFO_V1(trimmed_var_int32);
PG_FUNCTION_INFO_V1(trimmed_var_int64);
PG_FUNCTION_INFO_V1(trimmed_var_numeric);

Datum trimmed_var_double(PG_FUNCTION_ARGS);
Datum trimmed_var_int32(PG_FUNCTION_ARGS);
Datum trimmed_var_int64(PG_FUNCTION_ARGS);
Datum trimmed_var_numeric(PG_FUNCTION_ARGS);

/* population estimate */
PG_FUNCTION_INFO_V1(trimmed_var_pop_double);
PG_FUNCTION_INFO_V1(trimmed_var_pop_int32);
PG_FUNCTION_INFO_V1(trimmed_var_pop_int64);
PG_FUNCTION_INFO_V1(trimmed_var_pop_numeric);

Datum trimmed_var_pop_double(PG_FUNCTION_ARGS);
Datum trimmed_var_pop_int32(PG_FUNCTION_ARGS);
Datum trimmed_var_pop_int64(PG_FUNCTION_ARGS);
Datum trimmed_var_pop_numeric(PG_FUNCTION_ARGS);

/* sample estimate */
PG_FUNCTION_INFO_V1(trimmed_var_samp_double);
PG_FUNCTION_INFO_V1(trimmed_var_samp_int32);
PG_FUNCTION_INFO_V1(trimmed_var_samp_int64);
PG_FUNCTION_INFO_V1(trimmed_var_samp_numeric);

Datum trimmed_var_samp_double(PG_FUNCTION_ARGS);
Datum trimmed_var_samp_int32(PG_FUNCTION_ARGS);
Datum trimmed_var_samp_int64(PG_FUNCTION_ARGS);
Datum trimmed_var_samp_numeric(PG_FUNCTION_ARGS);

/* STANDARD DEVIATION */

/* exact */
PG_FUNCTION_INFO_V1(trimmed_stddev_double);
PG_FUNCTION_INFO_V1(trimmed_stddev_int32);
PG_FUNCTION_INFO_V1(trimmed_stddev_int64);
PG_FUNCTION_INFO_V1(trimmed_stddev_numeric);

Datum trimmed_stddev_double(PG_FUNCTION_ARGS);
Datum trimmed_stddev_int32(PG_FUNCTION_ARGS);
Datum trimmed_stddev_int64(PG_FUNCTION_ARGS);
Datum trimmed_stddev_numeric(PG_FUNCTION_ARGS);

/* population estimate */
PG_FUNCTION_INFO_V1(trimmed_stddev_pop_double);
PG_FUNCTION_INFO_V1(trimmed_stddev_pop_int32);
PG_FUNCTION_INFO_V1(trimmed_stddev_pop_int64);
PG_FUNCTION_INFO_V1(trimmed_stddev_pop_numeric);

Datum trimmed_stddev_pop_double(PG_FUNCTION_ARGS);
Datum trimmed_stddev_pop_int32(PG_FUNCTION_ARGS);
Datum trimmed_stddev_pop_int64(PG_FUNCTION_ARGS);
Datum trimmed_stddev_pop_numeric(PG_FUNCTION_ARGS);

/* sample estimate */
PG_FUNCTION_INFO_V1(trimmed_stddev_samp_double);
PG_FUNCTION_INFO_V1(trimmed_stddev_samp_int32);
PG_FUNCTION_INFO_V1(trimmed_stddev_samp_int64);
PG_FUNCTION_INFO_V1(trimmed_stddev_samp_numeric);

Datum trimmed_stddev_samp_double(PG_FUNCTION_ARGS);
Datum trimmed_stddev_samp_int32(PG_FUNCTION_ARGS);
Datum trimmed_stddev_samp_int64(PG_FUNCTION_ARGS);
Datum trimmed_stddev_samp_numeric(PG_FUNCTION_ARGS);

/* AVERAGE */

PG_FUNCTION_INFO_V1(trimmed_double_array);
PG_FUNCTION_INFO_V1(trimmed_int32_array);
PG_FUNCTION_INFO_V1(trimmed_int64_array);
PG_FUNCTION_INFO_V1(trimmed_numeric_array);

Datum trimmed_double_array(PG_FUNCTION_ARGS);
Datum trimmed_int32_array(PG_FUNCTION_ARGS);
Datum trimmed_int64_array(PG_FUNCTION_ARGS);
Datum trimmed_numeric_array(PG_FUNCTION_ARGS);

/* numeric helper */
static Numeric create_numeric(int value);
static Numeric add_numeric(Numeric a, Numeric b);
static Numeric sub_numeric(Numeric a, Numeric b);
static Numeric div_numeric(Numeric a, Numeric b);
static Numeric mul_numeric(Numeric a, Numeric b);
static Numeric pow_numeric(Numeric a, int b);
static Numeric sqrt_numeric(Numeric a);


Datum
trimmed_append_double(PG_FUNCTION_ARGS)
{
	state_double *state;
	MemoryContext aggcontext;

	GET_AGG_CONTEXT("trimmed_append_double", fcinfo, aggcontext);

	/*
	 * If both arguments are NULL, we can return NULL directly (instead of
	 * just allocating empty aggregate state even if we don't need it).
	 */
	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	if (PG_ARGISNULL(0))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(aggcontext);

		state = (state_double*)palloc(sizeof(state_double));
		state->elements = (double*)palloc(MIN_ELEMENTS * sizeof(double));

		MemoryContextSwitchTo(oldcontext);

		state->maxelements = MIN_ELEMENTS;
		state->nelements = 0;
		state->sorted = false;

		/* how much to cut */
		if (PG_ARGISNULL(2) || PG_ARGISNULL(3))
			elog(ERROR, "both upper and lower cut must not be NULL");

		state->cut_lower = PG_GETARG_FLOAT8(2);
		state->cut_upper = PG_GETARG_FLOAT8(3);

		if (state->cut_lower < 0.0 || state->cut_lower >= 1.0)
			elog(ERROR, "lower cut needs to be between 0 and 1 (inclusive)");

		if (state->cut_upper < 0.0 || state->cut_upper >= 1.0)
			elog(ERROR, "upper cut needs to be between 0 and 1 (inclusive)");

		if (state->cut_lower + state->cut_upper >= 1.0)
			elog(ERROR, "lower and upper cut sum to >= 1.0");
	}
	else
		state = (state_double*)PG_GETARG_POINTER(0);

	if (! PG_ARGISNULL(1))
	{
		double element = PG_GETARG_FLOAT8(1);

		if (state->nelements >= state->maxelements)
		{
			state->maxelements *= 2;
			state->elements = (double*)repalloc(state->elements,
											   sizeof(double) * state->maxelements);
		}

		state->elements[state->nelements++] = element;
	}

	Assert((state->nelements >= 0) && (state->nelements <= state->maxelements));

	PG_RETURN_POINTER(state);
}

Datum
trimmed_append_int32(PG_FUNCTION_ARGS)
{
	state_int32 *state;
	MemoryContext aggcontext;

	GET_AGG_CONTEXT("trimmed_append_int32", fcinfo, aggcontext);

	/*
	 * If both arguments are NULL, we can return NULL directly (instead of
	 * just allocating empty aggregate state even if we don't need it).
	 */
	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	if (PG_ARGISNULL(0))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(aggcontext);

		state = (state_int32*)palloc(sizeof(state_int32));
		state->elements = (int32*)palloc(MIN_ELEMENTS * sizeof(int32));

		MemoryContextSwitchTo(oldcontext);

		state->maxelements = MIN_ELEMENTS;
		state->nelements = 0;
		state->sorted = false;

		/* how much to cut */
		if (PG_ARGISNULL(2) || PG_ARGISNULL(3))
			elog(ERROR, "both upper and lower cut must not be NULL");

		state->cut_lower = PG_GETARG_FLOAT8(2);
		state->cut_upper = PG_GETARG_FLOAT8(3);

		if (state->cut_lower < 0.0 || state->cut_lower >= 1.0)
			elog(ERROR, "lower cut needs to be between 0 and 1 (inclusive)");

		if (state->cut_upper < 0.0 || state->cut_upper >= 1.0)
			elog(ERROR, "upper cut needs to be between 0 and 1 (inclusive)");

		if (state->cut_lower + state->cut_upper >= 1.0)
			elog(ERROR, "lower and upper cut sum to >= 1.0");
	}
	else
		state = (state_int32*)PG_GETARG_POINTER(0);

	if (! PG_ARGISNULL(1))
	{
		int32 element = PG_GETARG_INT32(1);

		if (state->nelements >= state->maxelements)
		{
			state->maxelements *= 2;
			state->elements = (int32*)repalloc(state->elements,
											  sizeof(int32) * state->maxelements);
		}

		state->elements[state->nelements++] = element;
	}

	Assert((state->nelements >= 0) && (state->nelements <= state->maxelements));

	PG_RETURN_POINTER(state);
}

Datum
trimmed_append_int64(PG_FUNCTION_ARGS)
{
	state_int64 *state;
	MemoryContext aggcontext;

	GET_AGG_CONTEXT("trimmed_append_int64", fcinfo, aggcontext);

	/*
	 * If both arguments are NULL, we can return NULL directly (instead of
	 * just allocating empty aggregate state even if we don't need it).
	 */
	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	if (PG_ARGISNULL(0))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(aggcontext);

		state = (state_int64*)palloc(sizeof(state_int64));
		state->elements = (int64*)palloc(MIN_ELEMENTS * sizeof(int64));

		MemoryContextSwitchTo(oldcontext);

		state->maxelements = MIN_ELEMENTS;
		state->nelements = 0;
		state->sorted = false;

		/* how much to cut */
		if (PG_ARGISNULL(2) || PG_ARGISNULL(3))
			elog(ERROR, "both upper and lower cut must not be NULL");

		state->cut_lower = PG_GETARG_FLOAT8(2);
		state->cut_upper = PG_GETARG_FLOAT8(3);

		if (state->cut_lower < 0.0 || state->cut_lower >= 1.0)
			elog(ERROR, "lower cut needs to be between 0 and 1 (inclusive)");

		if (state->cut_upper < 0.0 || state->cut_upper >= 1.0)
			elog(ERROR, "upper cut needs to be between 0 and 1 (inclusive)");

		if (state->cut_lower + state->cut_upper >= 1.0)
			elog(ERROR, "lower and upper cut sum to >= 1.0");
	}
	else
		state = (state_int64*)PG_GETARG_POINTER(0);

	if (! PG_ARGISNULL(1))
	{
		int64 element = PG_GETARG_INT64(1);

		if (state->nelements >= state->maxelements)
		{
			state->maxelements *= 2;
			state->elements = (int64*)repalloc(state->elements,
											  sizeof(int64) * state->maxelements);
		}

		state->elements[state->nelements++] = element;
	}

	Assert((state->nelements >= 0) && (state->nelements <= state->maxelements));

	PG_RETURN_POINTER(state);
}

Datum
trimmed_append_numeric(PG_FUNCTION_ARGS)
{
	state_numeric *state;
	MemoryContext aggcontext;

	GET_AGG_CONTEXT("trimmed_append_numeric", fcinfo, aggcontext);

	/*
	 * If both arguments are NULL, we can return NULL directly (instead of
	 * just allocating empty aggregate state even if we don't need it).
	 */
	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	if (PG_ARGISNULL(0))
	{
		state = (state_numeric*)MemoryContextAlloc(aggcontext,
												   sizeof(state_numeric));

		state->nelements = 0;
		state->data = NULL;
		state->usedlen = 0;
		state->maxlen = 32;	/* TODO make this a constant */
		state->sorted = false;

		/* how much to cut */
		if (PG_ARGISNULL(2) || PG_ARGISNULL(3))
			elog(ERROR, "both upper and lower cut must not be NULL");

		state->cut_lower = PG_GETARG_FLOAT8(2);
		state->cut_upper = PG_GETARG_FLOAT8(3);

		if (state->cut_lower < 0.0 || state->cut_lower >= 1.0)
			elog(ERROR, "lower cut needs to be between 0 and 1 (inclusive)");

		if (state->cut_upper < 0.0 || state->cut_upper >= 1.0)
			elog(ERROR, "upper cut needs to be between 0 and 1 (inclusive)");

		if (state->cut_lower + state->cut_upper >= 1.0)
			elog(ERROR, "lower and upper cut sum to >= 1.0");
	}
	else
		state = (state_numeric*)PG_GETARG_POINTER(0);

	if (! PG_ARGISNULL(1))
	{
		Numeric element = PG_GETARG_NUMERIC(1);

		int len = VARSIZE(element);

		/* if there's not enough space in the data buffer, repalloc it */
		if (state->usedlen + len > state->maxlen)
		{
			while (len + state->usedlen > state->maxlen)
				state->maxlen *= 2;

			if (state->data != NULL)
				state->data = repalloc(state->data, state->maxlen);
		}

		/* if first entry, we need to allocate the buffer */
		if (! state->data)
			state->data = MemoryContextAlloc(aggcontext, state->maxlen);

		/* copy the contents of the Numeric in place */
		memcpy(state->data + state->usedlen, element, len);

		state->usedlen += len;
		state->nelements += 1;
	}

	Assert(state->usedlen <= state->maxlen);
	Assert((!state->usedlen && !state->data) || (state->usedlen && state->data));

	PG_RETURN_POINTER(state);
}

Datum
trimmed_avg_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0;

	state_double *state;

	CHECK_AGG_CONTEXT("trimmed_avg_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_double*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_double(state);

	for (i = from; i < to; i++)
		result = result + state->elements[i];

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_double_array(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	/* average, var_pop, var_samp, variance, stddev_pop, stddev_samp, stddev */
	double  result[7] = {0, 0, 0, 0, 0, 0, 0};

	state_double *state;

	CHECK_AGG_CONTEXT("trimmed_double_array", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_double*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_double(state);

	/* average */
	result[0] = 0;
	result[1] = 1;
	result[2] = 2;

	for (i = from; i < to; i++)
	{
		result[0] += state->elements[i];
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	result[0] /= cnt;
	result[1] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt);	   /* var_pop */
	result[2] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)); /* var_samp */

	/* variance */
	result[3] = 0;
	for (i = from; i < to; i++)
		result[3] += (state->elements[i] - result[0]) * (state->elements[i] - result[0]);

	result[3] /= cnt;
	result[4] = sqrt(result[1]); /* stddev_pop */
	result[5] = sqrt(result[2]); /* stddev_samp */
	result[6] = sqrt(result[3]); /* stddev */

	return double_to_array(fcinfo, result, 7);
}

Datum
trimmed_avg_int32(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0;

	state_int32 *state;

	CHECK_AGG_CONTEXT("trimmed_avg_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int32(state);

	for (i = from; i < to; i++)
		result = result + (double)state->elements[i];

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_int32_array(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	/* average, var_pop, var_samp, variance, stddev_pop, stddev_samp, stddev */
	double	result[7] = {0, 0, 0, 0, 0, 0, 0};

	state_int32 *state;

	CHECK_AGG_CONTEXT("trimmed_int32_array", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int32(state);

	/* average */
	result[0] = 0;
	result[1] = 0;
	result[2] = 0;

	for (i = from; i < to; i++)
	{
		result[0] += (double)state->elements[i];
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + ((double)state->elements[i])*((double)state->elements[i]);
	}

	result[0] /= cnt;
	result[1] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt);	   /* var_pop */
	result[2] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)); /* var_samp */

	/* variance */
	result[3] = 0;
	for (i = from; i < to; i++)
		result[3] += ((double)state->elements[i] - result[0])*((double)state->elements[i] - result[0]);

	result[3] /= cnt;
	result[4] = sqrt(result[1]); /* stddev_pop */
	result[5] = sqrt(result[2]); /* stddev_samp */
	result[6] = sqrt(result[3]); /* stddev */

	return double_to_array(fcinfo, result, 7);
}

Datum
trimmed_avg_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0;

	state_int64 *state;

	CHECK_AGG_CONTEXT("trimmed_avg_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int64(state);

	for (i = from; i < to; i++)
		result = result + (double)state->elements[i];

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_int64_array(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	/* average, var_pop, var_samp, variance, stddev_pop, stddev_samp, stddev */
	double	result[7] = {0, 0, 0, 0, 0, 0, 0};

	state_int64 *state;

	CHECK_AGG_CONTEXT("trimmed_int64_array", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int64(state);

	/* average */
	result[0] = 0;
	result[1] = 0;
	result[2] = 0;

	for (i = from; i < to; i++)
	{
		result[0] += (double)state->elements[i];
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + ((double)state->elements[i])*((double)state->elements[i]);
	}

	result[0] /= cnt;
	result[1] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt);	   /* var_pop */
	result[2] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)); /* var_samp */

	/* variance */
	result[3] = 0;
	for (i = from; i < to; i++)
		result[3] += ((double)state->elements[i] - result[0])*((double)state->elements[i] - result[0]);

	result[3] /= cnt;
	result[4] = sqrt(result[1]); /* stddev_pop */
	result[5] = sqrt(result[2]); /* stddev_samp */
	result[6] = sqrt(result[3]); /* stddev */

	return double_to_array(fcinfo, result, 7);
}

Datum
trimmed_avg_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	result, cnt;
	char   *ptr;

	state_numeric *state;

	CHECK_AGG_CONTEXT("trimmed_avg_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	/* create numeric values */
	cnt	= create_numeric(to-from);
	result = create_numeric(0);

	sort_state_numeric(state);

	/* we need to walk through the buffer from start */
	for (i = 0, ptr = state->data; i < to; i++, ptr += VARSIZE(ptr))
	{
		Assert(ptr <= (state->data + state->usedlen));

		if (i >= from)
			result = add_numeric(result, div_numeric((Numeric)ptr, cnt));
	}

	PG_RETURN_NUMERIC(result);
}

Datum
trimmed_numeric_array(PG_FUNCTION_ARGS)
{
	int		i, from, to;

	/* average, var_pop, var_samp, variance, stddev_pop, stddev_samp, stddev */
	Numeric	result[7];
	Numeric	sum_x, sum_x2;
	Numeric	cntNumeric, cntNumeric_1;
	char *ptr, *fromptr;

	state_numeric *state;

	CHECK_AGG_CONTEXT("trimmed_numeric_array", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	/* create numeric values */
	cntNumeric = create_numeric(to-from);
	cntNumeric_1 = create_numeric(to-from-1);

	sort_state_numeric(state);

	/* average */
	result[0] = create_numeric(0);
	result[1] = create_numeric(0);
	result[2] = create_numeric(0);

	sum_x   = create_numeric(0);
	sum_x2  = create_numeric(0);

	/* compute sumX and sumX2 */
	for (i = 0, ptr = state->data, fromptr = NULL; i < to; i++, ptr += VARSIZE(ptr))
	{
		Assert(ptr <= (state->data + state->usedlen));

		if (i >= from)
		{
			/* remember offset to the 'from' value */
			fromptr = (i == from) ? ptr : fromptr;

			sum_x  = add_numeric(sum_x, (Numeric)ptr);
			sum_x2 = add_numeric(sum_x2,
								 mul_numeric((Numeric)ptr,
											 (Numeric)ptr));
		}
	}

	/* make sure we got a valid pointer to start from in the second pass */
	Assert(fromptr != NULL);

	/* compute the average */
	result[0] = div_numeric(sum_x, cntNumeric);

	/* var_pop */
	result[1] = div_numeric(
					sub_numeric(
						mul_numeric(cntNumeric, sum_x2),
						mul_numeric(sum_x, sum_x)
					),
					mul_numeric(cntNumeric, cntNumeric));

	/* var_samp */
	result[2] = div_numeric(
					sub_numeric(
						mul_numeric(cntNumeric, sum_x2),
						mul_numeric(sum_x, sum_x)
					),
					mul_numeric(cntNumeric, cntNumeric_1));

	/* variance */
	result[3] = create_numeric(0);
	for (i = from, ptr = fromptr; i < to; i++, ptr += VARSIZE(ptr))
	{
		Numeric	 delta;

		Assert(ptr <= (state->data + state->usedlen));

		delta = sub_numeric((Numeric)ptr, result[0]);
		result[3]   = add_numeric(result[3], mul_numeric(delta, delta));
	}
	result[3] = div_numeric(result[3], cntNumeric);

	result[4] = sqrt_numeric(result[1]); /* stddev_pop */
	result[5] = sqrt_numeric(result[2]); /* stddev_samp */
	result[6] = sqrt_numeric(result[3]); /* stddev */

	return numeric_to_array(fcinfo, result, 7);
}

Datum
trimmed_var_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_double *state;

	CHECK_AGG_CONTEXT("trimmed_var_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_double*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_double(state);

	for (i = from; i < to; i++)
		avg = avg + state->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (state->elements[i] - avg)*(state->elements[i] - avg);

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_var_int32(PG_FUNCTION_ARGS)
{

	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_int32 *state;

	CHECK_AGG_CONTEXT("trimmed_var_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int32(state);

	for (i = from; i < to; i++)
		avg = avg + (double)state->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (state->elements[i] - avg)*(state->elements[i] - avg);

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_var_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_int64 *state;

	CHECK_AGG_CONTEXT("trimmed_var_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int64(state);

	for (i = from; i < to; i++)
		avg = avg + (double)state->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (state->elements[i] - avg)*(state->elements[i] - avg);

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_var_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	result, avg, cnt;
	char   *ptr, *fromptr;
	state_numeric *state;

	CHECK_AGG_CONTEXT("trimmed_var_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	cnt = create_numeric(to - from);
	avg = create_numeric(0);
	result = create_numeric(0);

	sort_state_numeric(state);

	for (i = 0, ptr = state->data, fromptr = NULL; i < to; i++, ptr += VARSIZE(ptr))
	{
		Assert(ptr <= (state->data + state->usedlen));

		if (i >= from)
		{
			/* remember offset to the 'from' value */
			fromptr = (i == from) ? ptr : fromptr;

			avg = add_numeric(avg, div_numeric((Numeric)ptr, cnt));
		}
	}

	/* make sure we got a valid pointer for the second pass */
	Assert(fromptr != NULL);

	for (i = from, ptr = fromptr; i < to; i++, ptr += VARSIZE(ptr))
	{
		Assert(ptr <= (state->data + state->usedlen));

		result = add_numeric(
					result,
					div_numeric(
						pow_numeric(sub_numeric((Numeric)ptr,avg),2),
						cnt));
	}

	PG_RETURN_NUMERIC(result);
}

Datum
trimmed_var_pop_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0, numerator;

	state_double *state;

	CHECK_AGG_CONTEXT("trimmed_var_pop_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_double*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_double(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (numerator / (cnt * cnt));
}

Datum
trimmed_var_pop_int32(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0, numerator;

	state_int32 *state;

	CHECK_AGG_CONTEXT("trimmed_var_pop_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int32(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (numerator / (cnt * cnt));
}

Datum
trimmed_var_pop_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double  sum_x = 0, sum_x2 = 0, numerator;

	state_int64 *state;

	CHECK_AGG_CONTEXT("trimmed_var_pop_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int64(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (numerator / (cnt * cnt));
}

Datum
trimmed_var_pop_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	sum_x, sum_x2, cnt, numerator;
	char   *ptr;
	state_numeric *state;

	CHECK_AGG_CONTEXT("trimmed_var_pop_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	cnt = create_numeric(to - from);
	sum_x = create_numeric(0);
	sum_x2 = create_numeric(0);

	sort_state_numeric(state);

	for (i = 0, ptr = state->data; i < to; i++, ptr += VARSIZE(ptr))
	{
		Assert(ptr <= (state->data + state->usedlen));

		sum_x = add_numeric(sum_x, (Numeric)ptr);
		sum_x2 = add_numeric(
					sum_x2,
					mul_numeric((Numeric)ptr, (Numeric)ptr));
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = sub_numeric(
					mul_numeric(cnt, sum_x2),
					mul_numeric(sum_x, sum_x));

	if (numeric_comparator(&numerator, numeric_zero()) <= 0)
		PG_RETURN_NUMERIC(const_zero);

	PG_RETURN_NUMERIC (div_numeric(
							numerator,
							mul_numeric(cnt, cnt)));
}

Datum
trimmed_var_samp_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0, numerator;

	state_double *state;

	CHECK_AGG_CONTEXT("trimmed_var_samp_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_double*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_double(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (numerator / (cnt * (cnt - 1)));
}

Datum
trimmed_var_samp_int32(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0, numerator;

	state_int32 *state;

	CHECK_AGG_CONTEXT("trimmed_var_samp_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int32(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (numerator / (cnt * (cnt - 1)));
}

Datum
trimmed_var_samp_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0, numerator;

	state_int64 *state;

	CHECK_AGG_CONTEXT("trimmed_var_samp_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int64(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (numerator / (cnt * (cnt - 1)));
}

Datum
trimmed_var_samp_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	sum_x, sum_x2, cnt, numerator;
	char   *ptr;
	state_numeric *state;

	CHECK_AGG_CONTEXT("trimmed_var_samp_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	cnt  = create_numeric(to - from);
	sum_x = create_numeric(0);
	sum_x2 = create_numeric(0);

	sort_state_numeric(state);

	for (i = 0, ptr = state->data; i < to; i++, ptr += VARSIZE(ptr))
	{
		Assert(ptr <= (state->data + state->usedlen));

		sum_x = add_numeric(sum_x, (Numeric)ptr);
		sum_x2 = add_numeric(
						sum_x2,
						mul_numeric((Numeric)ptr, (Numeric)ptr));
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = sub_numeric(
					mul_numeric(cnt, sum_x2),
					mul_numeric(sum_x, sum_x));

	if (numeric_comparator(&numerator, numeric_zero()) <= 0)
		PG_RETURN_NUMERIC(const_zero);

	PG_RETURN_NUMERIC (div_numeric(
							numerator,
							mul_numeric(
								cnt,
								sub_numeric(cnt, create_numeric(1)))));
}

Datum
trimmed_stddev_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_double *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_double*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_double(state);

	for (i = from; i < to; i++)
		avg = avg + state->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (state->elements[i] - avg)*(state->elements[i] - avg);

	PG_RETURN_FLOAT8 (sqrt(result)/cnt);
}

Datum
trimmed_stddev_int32(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_int32 *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int32(state);

	for (i = from; i < to; i++)
		avg = avg + (double)state->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (state->elements[i] - avg)*(state->elements[i] - avg);

	PG_RETURN_FLOAT8 (sqrt(result)/cnt);
}

Datum
trimmed_stddev_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_int64 *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int64(state);

	for (i = from; i < to; i++)
		avg = avg + (double)state->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (state->elements[i] - avg)*(state->elements[i] - avg);

	PG_RETURN_FLOAT8 (sqrt(result)/cnt);
}

Datum
trimmed_stddev_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	result, avg, cnt;
	char   *ptr, *fromptr;
	state_numeric *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	cnt = create_numeric(to - from);
	avg = create_numeric(0);
	result = create_numeric(0);

	sort_state_numeric(state);

	for (i = 0, ptr = state->data, fromptr = NULL; i < to; i++, ptr += VARSIZE(ptr))
	{
		Assert(ptr <= (state->data + state->usedlen));

		if (i >= from)
		{
			/* remember offset to the 'from' value */
			fromptr = (i == from) ? ptr : fromptr;

			avg = add_numeric(avg, div_numeric((Numeric)ptr, cnt));
		}
	}

	/* make sure we have a valid start pointer for the second pass */
	Assert(fromptr != NULL);

	for (i = from, ptr = fromptr; i < to; i++, ptr += VARSIZE(ptr))
	{
		Assert(ptr <= (state->data + state->usedlen));

		result = add_numeric(
					result,
					div_numeric(
						pow_numeric(sub_numeric((Numeric)ptr, avg), 2),
						cnt));
	}

	PG_RETURN_NUMERIC (sqrt_numeric(result));
}

Datum
trimmed_stddev_pop_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0, numerator;

	state_double *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_pop_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_double*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_double(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (sqrt(numerator / (cnt * cnt)));
}

Datum
trimmed_stddev_pop_int32(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0, numerator;

	state_int32 *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_pop_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int32(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (sqrt(numerator / (cnt * cnt)));
}

Datum
trimmed_stddev_pop_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0, numerator;

	state_int64 *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_pop_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int64(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (sqrt(numerator / (cnt * cnt)));
}

Datum
trimmed_stddev_pop_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	sum_x, sum_x2, cnt, numerator;
	char   *ptr;
	state_numeric *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_pop_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	cnt  = create_numeric(to - from);
	sum_x = create_numeric(0);
	sum_x2 = create_numeric(0);

	sort_state_numeric(state);

	for (i = 0, ptr = state->data; i < to; i++, ptr += VARSIZE(ptr))
	{
		Assert(ptr <= (state->data + state->usedlen));

		sum_x = add_numeric(sum_x, (Numeric)ptr);
		sum_x2 = add_numeric(sum_x2,
							 mul_numeric((Numeric)ptr, (Numeric)ptr));
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = sub_numeric(
						mul_numeric(cnt, sum_x2),
						mul_numeric(sum_x, sum_x));

	if (numeric_comparator(&numerator, numeric_zero()) <= 0)
		PG_RETURN_NUMERIC(const_zero);

	PG_RETURN_NUMERIC (sqrt_numeric(
							div_numeric(
								numerator,
								pow_numeric(cnt, 2))));
}

Datum
trimmed_stddev_samp_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0, numerator;

	state_double *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_samp_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_double*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_double(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (sqrt(numerator / (cnt * (cnt - 1))));
}

Datum
trimmed_stddev_samp_int32(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0, numerator;

	state_int32 *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_samp_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int32(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (sqrt(numerator / (cnt * (cnt - 1))));
}

Datum
trimmed_stddev_samp_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0, numerator;

	state_int64 *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_samp_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);
	cnt  = (to - from);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	sort_state_int64(state);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + state->elements[i];
		sum_x2 = sum_x2 + state->elements[i]*state->elements[i];
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = (cnt * sum_x2 - sum_x * sum_x);
	if (numerator <= 0)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8 (sqrt(numerator / (cnt * (cnt - 1))));
}

Datum
trimmed_stddev_samp_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	sum_x, sum_x2, cnt, numerator;
	char   *ptr;
	state_numeric *state;

	CHECK_AGG_CONTEXT("trimmed_stddev_samp_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(state->nelements * state->cut_lower);
	to   = state->nelements - floor(state->nelements * state->cut_upper);

	Assert((0 <= from) && (from <= to) && (to <= state->nelements));

	if (from >= to)
		PG_RETURN_NULL();

	cnt  = create_numeric(to - from);
	sum_x = create_numeric(0);
	sum_x2 = create_numeric(0);

	sort_state_numeric(state);

	for (i = 0, ptr = state->data; i < to; i++, ptr += VARSIZE(ptr))
	{
		Assert(ptr <= (state->data + state->usedlen));

		sum_x = add_numeric(sum_x, (Numeric)ptr);
		sum_x2 = add_numeric(sum_x2, pow_numeric((Numeric)ptr, 2));
	}

	/* Watch out for roundoff error producing a negative numerator */
	numerator = sub_numeric(
						mul_numeric(cnt, sum_x2),
						pow_numeric(sum_x, 2));

	if (numeric_comparator(&numerator, numeric_zero()) <= 0)
		PG_RETURN_NUMERIC(const_zero);

	PG_RETURN_NUMERIC (sqrt_numeric(
							div_numeric(
								numerator,
								mul_numeric(
									cnt,
									sub_numeric(cnt, create_numeric(1))))));
}

static int
double_comparator(const void *a, const void *b)
{
	double af = (*(double*)a);
	double bf = (*(double*)b);
	return (af > bf) - (af < bf);
}

static int
int32_comparator(const void *a, const void *b)
{
	int32 af = (*(int32*)a);
	int32 bf = (*(int32*)b);
	return (af > bf) - (af < bf);
}

static int
int64_comparator(const void *a, const void *b)
{
	int64 af = (*(int64*)a);
	int64 bf = (*(int64*)b);
	return (af > bf) - (af < bf);
}

static int
numeric_comparator(const void *a, const void *b)
{
	FunctionCallInfoData fcinfo;

	/* set params */
	fcinfo.nargs = 2;
	fcinfo.arg[0] = NumericGetDatum(*(Numeric*)a);
	fcinfo.arg[1] = NumericGetDatum(*(Numeric*)b);
	fcinfo.argnull[0] = false;
	fcinfo.argnull[1] = false;

	/* return the result */
	return DatumGetInt32(numeric_cmp(&fcinfo));
}

static Numeric
create_numeric(int value)
{
	FunctionCallInfoData fcinfo;

	/* set params */
	fcinfo.nargs = 1;
	fcinfo.arg[0] = Int32GetDatum(value);
	fcinfo.argnull[0] = false;

	/* return the result */
	return DatumGetNumeric(int4_numeric(&fcinfo));
}

static Numeric
add_numeric(Numeric a, Numeric b)
{
	FunctionCallInfoData fcinfo;

	/* set params */
	fcinfo.nargs = 2;
	fcinfo.arg[0] = NumericGetDatum(a);
	fcinfo.arg[1] = NumericGetDatum(b);
	fcinfo.argnull[0] = false;
	fcinfo.argnull[1] = false;

	/* return the result */
	return DatumGetNumeric(numeric_add(&fcinfo));
}

static Numeric
div_numeric(Numeric a, Numeric b)
{
	FunctionCallInfoData fcinfo;

	/* set params */
	fcinfo.nargs = 2;
	fcinfo.arg[0] = NumericGetDatum(a);
	fcinfo.arg[1] = NumericGetDatum(b);
	fcinfo.argnull[0] = false;
	fcinfo.argnull[1] = false;

	/* return the result */
	return DatumGetNumeric(numeric_div(&fcinfo));
}

static Numeric
mul_numeric(Numeric a, Numeric b)
{
	FunctionCallInfoData fcinfo;

	/* set params */
	fcinfo.nargs = 2;
	fcinfo.arg[0] = NumericGetDatum(a);
	fcinfo.arg[1] = NumericGetDatum(b);
	fcinfo.argnull[0] = false;
	fcinfo.argnull[1] = false;

	/* return the result */
	return DatumGetNumeric(numeric_mul(&fcinfo));
}

static Numeric
sub_numeric(Numeric a, Numeric b)
{
	FunctionCallInfoData fcinfo;

	/* set params */
	fcinfo.nargs = 2;
	fcinfo.arg[0] = NumericGetDatum(a);
	fcinfo.arg[1] = NumericGetDatum(b);
	fcinfo.argnull[0] = false;
	fcinfo.argnull[1] = false;

	/* return the result */
	return DatumGetNumeric(numeric_sub(&fcinfo));
}

static Numeric
pow_numeric(Numeric a, int b)
{
	FunctionCallInfoData fcinfo;

	/* set params */
	fcinfo.nargs = 2;
	fcinfo.arg[0] = NumericGetDatum(a);
	fcinfo.arg[1] = NumericGetDatum(create_numeric(b));
	fcinfo.argnull[0] = false;
	fcinfo.argnull[1] = false;

	/* return the result */
	return DatumGetNumeric(numeric_power(&fcinfo));
}

static Numeric
sqrt_numeric(Numeric a)
{
	FunctionCallInfoData fcinfo;

	/* set params */
	fcinfo.nargs = 1;
	fcinfo.arg[0] = NumericGetDatum(a);
	fcinfo.argnull[0] = false;

	/* return the result */
	return DatumGetNumeric(numeric_sqrt(&fcinfo));
}

static Numeric *
numeric_zero()
{
	Datum d;

	if (const_zero != NULL)
		return &const_zero;

	d = DirectFunctionCall1(
			numeric_in,
			DirectFunctionCall1(
				int4out,
				Int32GetDatum(0)
			)
		);

	/* we need to copy the value to TopMemoryContext */
	const_zero = (Numeric)MemoryContextAlloc(TopMemoryContext, VARSIZE(d));

	memcpy(const_zero, DatumGetPointer(d), VARSIZE(d));

	return &const_zero;
}

/*
 * Helper functions used to prepare the resulting array (when there's
 * an array of quantiles).
 */
static Datum
double_to_array(FunctionCallInfo fcinfo, double * d, int len)
{
	ArrayBuildState *astate = NULL;
	int		 i;

	for (i = 0; i < len; i++) {

		/* stash away this field */
		astate = accumArrayResult(astate,
								  Float8GetDatum(d[i]),
								  false,
								  FLOAT8OID,
								  CurrentMemoryContext);
	}

	PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate,
										  CurrentMemoryContext));
}

static Datum
numeric_to_array(FunctionCallInfo fcinfo, Numeric * d, int len)
{
	ArrayBuildState *astate = NULL;
	int		 i;

	for (i = 0; i < len; i++) {

		/* stash away this field */
		astate = accumArrayResult(astate,
								  NumericGetDatum(d[i]),
								  false,
								  NUMERICOID,
								  CurrentMemoryContext);

	}

	PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate,
										  CurrentMemoryContext));
}

static void
sort_state_double(state_double *state)
{
	if (state->sorted)
		return;

	pg_qsort(state->elements, state->nelements, sizeof(double), &double_comparator);
	state->sorted = true;
}

static void
sort_state_int32(state_int32 *state)
{
	if (state->sorted)
		return;

	pg_qsort(state->elements, state->nelements, sizeof(int32), &int32_comparator);
	state->sorted = true;
}

static void
sort_state_int64(state_int64 *state)
{
	if (state->sorted)
		return;

	pg_qsort(state->elements, state->nelements, sizeof(int64), &int64_comparator);
	state->sorted = true;
}

static void
sort_state_numeric(state_numeric *state)
{
	int		i;
	char   *data;
	char   *ptr;
	Numeric *items;

	if (state->sorted)
		return;

	/*
	 * we'll sort a local copy of the data, and then copy it back (we want
	 * to put the result into the proper memory context)
	 */
	items = (Numeric*)palloc(sizeof(Numeric) * state->nelements);
	data = palloc(state->usedlen);
	memcpy(data, state->data, state->usedlen);

	/* parse the data into array of Numeric items, for pg_qsort */
	i = 0;
	ptr = data;
	while (ptr < data + state->usedlen)
	{
		items[i++] = (Numeric)ptr;
		ptr += VARSIZE(ptr);

		Assert(i <= state->nelements);
		Assert(ptr <= (data + state->usedlen));
	}

	Assert(i == state->nelements);
	Assert(ptr == (data + state->usedlen));

	pg_qsort(items, state->nelements, sizeof(Numeric), &numeric_comparator);

	/* copy the values from the local array back into the state */
	ptr = state->data;
	for (i = 0; i < state->nelements; i++)
	{
		memcpy(ptr, items[i], VARSIZE(items[i]));
		ptr += VARSIZE(items[i]);

		Assert(ptr <= state->data + state->usedlen);
	}

	Assert(ptr == state->data + state->usedlen);

	pfree(items);
	pfree(data);

	state->sorted = true;
}
