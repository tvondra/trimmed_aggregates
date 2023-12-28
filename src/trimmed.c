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

/* SERIALIZE STATE */

PG_FUNCTION_INFO_V1(trimmed_serial_double);
PG_FUNCTION_INFO_V1(trimmed_serial_int32);
PG_FUNCTION_INFO_V1(trimmed_serial_int64);
PG_FUNCTION_INFO_V1(trimmed_serial_numeric);

Datum trimmed_serial_double(PG_FUNCTION_ARGS);
Datum trimmed_serial_int32(PG_FUNCTION_ARGS);
Datum trimmed_serial_int64(PG_FUNCTION_ARGS);
Datum trimmed_serial_numeric(PG_FUNCTION_ARGS);

/* DESERIALIZE STATE */

PG_FUNCTION_INFO_V1(trimmed_deserial_double);
PG_FUNCTION_INFO_V1(trimmed_deserial_int32);
PG_FUNCTION_INFO_V1(trimmed_deserial_int64);
PG_FUNCTION_INFO_V1(trimmed_deserial_numeric);

Datum trimmed_deserial_double(PG_FUNCTION_ARGS);
Datum trimmed_deserial_int32(PG_FUNCTION_ARGS);
Datum trimmed_deserial_int64(PG_FUNCTION_ARGS);
Datum trimmed_deserial_numeric(PG_FUNCTION_ARGS);

/* COMBINE STATE */

PG_FUNCTION_INFO_V1(trimmed_combine_double);
PG_FUNCTION_INFO_V1(trimmed_combine_int32);
PG_FUNCTION_INFO_V1(trimmed_combine_int64);
PG_FUNCTION_INFO_V1(trimmed_combine_numeric);

Datum trimmed_combine_double(PG_FUNCTION_ARGS);
Datum trimmed_combine_int32(PG_FUNCTION_ARGS);
Datum trimmed_combine_int64(PG_FUNCTION_ARGS);
Datum trimmed_combine_numeric(PG_FUNCTION_ARGS);

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
trimmed_serial_double(PG_FUNCTION_ARGS)
{
	state_double   *state = (state_double *)PG_GETARG_POINTER(0);
	Size			hlen = offsetof(state_double, elements);	/* header */
	Size			len = state->nelements * sizeof(double);		/* elements */
	bytea		   *out = (bytea *)palloc(VARHDRSZ + len + hlen);
	char		   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_double", fcinfo);

	/* we want to serialize the data in sorted format */
	sort_state_double(state);

	SET_VARSIZE(out, VARHDRSZ + len + hlen);
	ptr = VARDATA(out);

	memcpy(ptr, state, offsetof(state_double, elements));
	ptr += offsetof(state_double, elements);

	memcpy(ptr, state->elements, len);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_serial_int32(PG_FUNCTION_ARGS)
{
	state_int32	   *state = (state_int32 *)PG_GETARG_POINTER(0);
	Size			hlen = offsetof(state_int32, elements);		/* header */
	Size			len = state->nelements * sizeof(int32);		/* elements */
	bytea		   *out = (bytea *)palloc(VARHDRSZ + len + hlen);
	char		   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_int32", fcinfo);

	/* we want to serialize the data in sorted format */
	sort_state_int32(state);

	SET_VARSIZE(out, VARHDRSZ + len + hlen);
	ptr = VARDATA(out);

	memcpy(ptr, state, offsetof(state_int32, elements));
	ptr += offsetof(state_int32, elements);

	memcpy(ptr, state->elements, len);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_serial_int64(PG_FUNCTION_ARGS)
{
	state_int64	   *state = (state_int64 *)PG_GETARG_POINTER(0);
	Size			hlen = offsetof(state_int64, elements);		/* header */
	Size			len = state->nelements * sizeof(int64);		/* elements */
	bytea		   *out = (bytea *)palloc(VARHDRSZ + len + hlen);
	char		   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_int64", fcinfo);

	/* we want to serialize the data in sorted format */
	sort_state_int64(state);

	SET_VARSIZE(out, VARHDRSZ + len + hlen);
	ptr = VARDATA(out);

	memcpy(ptr, state, offsetof(state_int64, elements));
	ptr += offsetof(state_int64, elements);

	memcpy(ptr, state->elements, len);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_serial_numeric(PG_FUNCTION_ARGS)
{
	state_numeric *state = (state_numeric *)PG_GETARG_POINTER(0);
	Size	hlen = offsetof(state_numeric, data);		/* header */
	Size	len = state->usedlen;						/* elements */
	bytea  *out;
	char   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_numeric", fcinfo);

	out = (bytea *)palloc(VARHDRSZ + len + hlen);
	SET_VARSIZE(out, VARHDRSZ + len + hlen);
	ptr = (char*) VARDATA(out);

	/* we want to serialize the data in sorted format */
	sort_state_numeric(state);

	/* now copy as a single chunk */
	memcpy(ptr, state, offsetof(state_numeric, data));
	ptr += offsetof(state_numeric, data);

	memcpy(ptr, state->data, state->usedlen);
	ptr += state->usedlen;

	/* we better get exactly the expected amount of data */
	Assert((char*)VARDATA(out) + len + hlen == ptr);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_deserial_double(PG_FUNCTION_ARGS)
{
	state_double *out = (state_double *)palloc(sizeof(state_double));
	bytea  *state = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(state);
	char   *ptr = VARDATA(state);

	CHECK_AGG_CONTEXT("trimmed_deserial_double", fcinfo);

	Assert(len > 0);
	Assert((len - offsetof(state_double, elements)) % sizeof(double) == 0);

	/* copy the header */
	memcpy(out, ptr, offsetof(state_double, elements));
	ptr += offsetof(state_double, elements);

	Assert((out->nelements > 0) && (out->maxelements >= out->nelements));
	Assert(len == offsetof(state_double, elements) + out->nelements * sizeof(double));
	Assert(out->sorted);

	/* we only allocate the necessary space */
	out->elements = (double *)palloc(out->nelements * sizeof(double));
	out->maxelements = out->nelements;

	memcpy((void *)out->elements, ptr, out->nelements * sizeof(double));

	PG_RETURN_POINTER(out);
}

Datum
trimmed_deserial_int32(PG_FUNCTION_ARGS)
{
	state_int32 *out = (state_int32 *)palloc(sizeof(state_int32));
	bytea  *state = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(state);
	char   *ptr = VARDATA(state);

	CHECK_AGG_CONTEXT("trimmed_deserial_int32", fcinfo);

	Assert(len > 0);
	Assert((len - offsetof(state_int32, elements)) % sizeof(int32) == 0);

	/* copy the header */
	memcpy(out, ptr, offsetof(state_int32, elements));
	ptr += offsetof(state_int32, elements);

	Assert((out->nelements > 0) && (out->maxelements >= out->nelements));
	Assert(len == offsetof(state_int32, elements) + out->nelements * sizeof(int32));
	Assert(out->sorted);

	/* we only allocate the necessary space */
	out->elements = (int32 *)palloc(out->nelements * sizeof(int32));
	out->maxelements = out->nelements;

	memcpy((void *)out->elements, ptr, out->nelements * sizeof(int32));

	PG_RETURN_POINTER(out);
}

Datum
trimmed_deserial_int64(PG_FUNCTION_ARGS)
{
	state_int64 *out = (state_int64 *)palloc(sizeof(state_int64));
	bytea  *state = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(state);
	char   *ptr = VARDATA(state);

	CHECK_AGG_CONTEXT("trimmed_deserial_int64", fcinfo);

	Assert(len > 0);
	Assert((len - offsetof(state_int64, elements)) % sizeof(int32) == 0);

	/* copy the header */
	memcpy(out, ptr, offsetof(state_int64, elements));
	ptr += offsetof(state_int64, elements);

	Assert((out->nelements > 0) && (out->maxelements >= out->nelements));
	Assert(len == offsetof(state_int64, elements) + out->nelements * sizeof(int64));
	Assert(out->sorted);

	/* we only allocate the necessary space */
	out->elements = (int64 *)palloc(out->nelements * sizeof(int64));
	out->maxelements = out->nelements;

	memcpy((void *)out->elements, ptr, out->nelements * sizeof(int64));

	PG_RETURN_POINTER(out);
}

Datum
trimmed_deserial_numeric(PG_FUNCTION_ARGS)
{
	state_numeric *out = (state_numeric *)palloc(sizeof(state_numeric));
	bytea  *state = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(state);
	char   *ptr = VARDATA(state);

	CHECK_AGG_CONTEXT("trimmed_deserial_numeric", fcinfo);

	Assert(len > 0);

	/* first read the struct header, stored at the beginning */
	memcpy(out, ptr, offsetof(state_numeric, data));
	ptr += offsetof(state_numeric, data);

	/* we don't serialize empty groups (we keep the state NULL) */
	Assert((out->nelements > 0) && (out->usedlen > 0));
	Assert(out->usedlen <= out->maxlen);
	Assert(out->usedlen == (len - offsetof(state_numeric, data)));
	Assert(out->sorted);

	/* fist copy the Numeric values into the buffer */
	out->data = palloc(len - offsetof(state_numeric, data));
	memcpy(out->data, ptr, len - offsetof(state_numeric, data));

	PG_RETURN_POINTER(out);
}

Datum
trimmed_combine_double(PG_FUNCTION_ARGS)
{
	int i, j, k;
	double *tmp;
	state_double *state1;
	state_double *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_double", fcinfo, agg_context);

	state1 = PG_ARGISNULL(0) ? NULL : (state_double *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (state_double *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = (state_double *)palloc(sizeof(state_double));
		state1->maxelements = state2->maxelements;
		state1->nelements = state2->nelements;

		state1->cut_lower = state2->cut_lower;
		state1->cut_upper = state2->cut_upper;
		state1->sorted = state2->sorted;

		state1->elements = (double*)palloc(sizeof(double) * state2->maxelements);

		memcpy(state1->elements, state2->elements, sizeof(double) * state2->maxelements);

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	Assert((state1 != NULL) && (state2 != NULL));

	/* make sure both states are sorted */
	sort_state_double(state1);
	sort_state_double(state2);

	tmp = (double*)MemoryContextAlloc(agg_context,
					  sizeof(double) * (state1->nelements + state2->nelements));

	/* merge the two arrays */
	i = j = k = 0;
	while (true)
	{
		Assert(k <= (state1->nelements + state2->nelements));
		Assert((i <= state1->nelements) && (j <= state2->nelements));

		if ((i < state1->nelements) && (j < state2->nelements))
		{
			if (state1->elements[i] <= state2->elements[j])
				tmp[k++] = state1->elements[i++];
			else
				tmp[k++] = state2->elements[j++];
		}
		else if (i < state1->nelements)
			tmp[k++] = state1->elements[i++];
		else if (j < state2->nelements)
			tmp[k++] = state2->elements[j++];
		else
			/* no more elements to process */
			break;
	}

	Assert(k == (state1->nelements + state2->nelements));
	Assert((i == state1->nelements) && (j == state2->nelements));

	/* free the two arrays */
	pfree(state1->elements);
	state1->elements = tmp;

	/* and finally remember the current number of elements */
	state1->nelements += state2->nelements;
	state1->maxelements = state1->nelements;

	PG_RETURN_POINTER(state1);
}

Datum
trimmed_combine_int32(PG_FUNCTION_ARGS)
{
	int i, j, k;
	int32 *tmp;
	state_int32 *state1;
	state_int32 *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_int32", fcinfo, agg_context);

	state1 = PG_ARGISNULL(0) ? NULL : (state_int32 *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (state_int32 *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = (state_int32 *)palloc(sizeof(state_int32));
		state1->maxelements = state2->maxelements;
		state1->nelements = state2->nelements;

		state1->cut_lower = state2->cut_lower;
		state1->cut_upper = state2->cut_upper;
		state1->sorted = state2->sorted;

		state1->elements = (int32*)palloc(sizeof(int32) * state2->maxelements);

		memcpy(state1->elements, state2->elements, sizeof(int32) * state2->maxelements);

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	Assert((state1 != NULL) && (state2 != NULL));

	/* make sure both states are sorted */
	sort_state_int32(state1);
	sort_state_int32(state2);

	tmp = (int32*)MemoryContextAlloc(agg_context,
					  sizeof(int32) * (state1->nelements + state2->nelements));

	/* merge the two arrays */
	i = j = k = 0;
	while (true)
	{
		Assert(k <= (state1->nelements + state2->nelements));
		Assert((i <= state1->nelements) && (j <= state2->nelements));

		if ((i < state1->nelements) && (j < state2->nelements))
		{
			if (state1->elements[i] <= state2->elements[j])
				tmp[k++] = state1->elements[i++];
			else
				tmp[k++] = state2->elements[j++];
		}
		else if (i < state1->nelements)
			tmp[k++] = state1->elements[i++];
		else if (j < state2->nelements)
			tmp[k++] = state2->elements[j++];
		else
			/* no more elements to process */
			break;
	}

	Assert(k == (state1->nelements + state2->nelements));
	Assert((i == state1->nelements) && (j == state2->nelements));

	/* free the two arrays */
	pfree(state1->elements);
	state1->elements = tmp;

	/* and finally remember the current number of elements */
	state1->nelements += state2->nelements;
	state1->maxelements = state1->nelements;

	PG_RETURN_POINTER(state1);
}

Datum
trimmed_combine_int64(PG_FUNCTION_ARGS)
{
	int i, j, k;
	int64 *tmp;
	state_int64 *state1;
	state_int64 *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_int64", fcinfo, agg_context);

	state1 = PG_ARGISNULL(0) ? NULL : (state_int64 *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (state_int64 *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = (state_int64 *)palloc(sizeof(state_int64));
		state1->maxelements = state2->maxelements;
		state1->nelements = state2->nelements;

		state1->cut_lower = state2->cut_lower;
		state1->cut_upper = state2->cut_upper;
		state1->sorted = state2->sorted;

		state1->elements = (int64*)palloc(sizeof(int64) * state2->maxelements);

		memcpy(state1->elements, state2->elements, sizeof(int64) * state2->maxelements);

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	Assert((state1 != NULL) && (state2 != NULL));

	/* make sure both states are sorted */
	sort_state_int64(state1);
	sort_state_int64(state2);

	tmp = (int64*)MemoryContextAlloc(agg_context,
					  sizeof(int64) * (state1->nelements + state2->nelements));

	/* merge the two arrays */
	i = j = k = 0;
	while (true)
	{
		Assert(k <= (state1->nelements + state2->nelements));
		Assert((i <= state1->nelements) && (j <= state2->nelements));

		if ((i < state1->nelements) && (j < state2->nelements))
		{
			if (state1->elements[i] <= state2->elements[j])
				tmp[k++] = state1->elements[i++];
			else
				tmp[k++] = state2->elements[j++];
		}
		else if (i < state1->nelements)
			tmp[k++] = state1->elements[i++];
		else if (j < state2->nelements)
			tmp[k++] = state2->elements[j++];
		else
			/* no more elements to process */
			break;
	}

	Assert(k == (state1->nelements + state2->nelements));
	Assert((i == state1->nelements) && (j == state2->nelements));

	/* free the two arrays */
	pfree(state1->elements);
	state1->elements = tmp;

	/* and finally remember the current number of elements */
	state1->nelements += state2->nelements;
	state1->maxelements = state1->nelements;

	PG_RETURN_POINTER(state1);
}

Datum
trimmed_combine_numeric(PG_FUNCTION_ARGS)
{
	int				i;
	state_numeric *state1;
	state_numeric *state2;
	MemoryContext agg_context;

	char		   *data, *tmp, *ptr1, *ptr2;

	GET_AGG_CONTEXT("trimmed_combine_numeric", fcinfo, agg_context);

	state1 = PG_ARGISNULL(0) ? NULL : (state_numeric *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (state_numeric *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	if (state1 == NULL)
	{
		state1 = (state_numeric *)MemoryContextAlloc(agg_context,
													sizeof(state_numeric));

		state1->nelements = state2->nelements;
		state1->cut_lower = state2->cut_lower;
		state1->cut_upper = state2->cut_upper;
		state1->usedlen = state2->usedlen;
		state1->maxlen = state2->maxlen;
		state1->sorted = state2->sorted;

		/* copy the buffer */
		state1->data = MemoryContextAlloc(agg_context, state1->usedlen);
		memcpy(state1->data, state2->data, state1->usedlen);

		PG_RETURN_POINTER(state1);
	}

	Assert((state1 != NULL) && (state2 != NULL));

	/* make sure both states are sorted */
	sort_state_numeric(state1);
	sort_state_numeric(state2);

	/* allocate temporary arrays */
	data = MemoryContextAlloc(agg_context, state1->usedlen + state2->usedlen);
	tmp = data;

	/* merge the two arrays */
	ptr1 = state1->data;
	ptr2 = state2->data;

	for (i = 0; i < state1->nelements + state2->nelements; i++)
	{
		Numeric element;

		Assert(ptr1 <= (state1->data + state1->usedlen));
		Assert(ptr2 <= (state2->data + state2->usedlen));

		if ((ptr1 < (state1->data + state1->usedlen)) &&
			(ptr2 < (state2->data + state2->usedlen)))
		{
			if (numeric_comparator(&ptr1, &ptr2) <= 0)
			{
				element = (Numeric)ptr1;
				ptr1 += VARSIZE(ptr1);
			}
			else
			{
				element = (Numeric)ptr2;
				ptr2 += VARSIZE(ptr2);
			}
		}
		else if (ptr1 < (state1->data + state1->usedlen))
		{
			element = (Numeric)ptr1;
			ptr1 += VARSIZE(ptr1);
		}
		else if (ptr2 < (state2->data + state2->usedlen))
		{
			element = (Numeric)ptr2;
			ptr2 += VARSIZE(ptr2);
		}
		else
			elog(ERROR, "unexpected");

		/* actually copy the value */
		memcpy(tmp, element, VARSIZE(element));
		tmp += VARSIZE(element);
	}

	Assert(ptr1 == (state1->data + state1->usedlen));
	Assert(ptr2 == (state2->data + state2->usedlen));
	Assert((tmp - data) == (state1->usedlen + state2->usedlen));

	/* free the two arrays */
	pfree(state1->data);
	state1->data = data;

	/* and finally remember the current number of elements */
	state1->nelements += state2->nelements;
	state1->usedlen += state2->usedlen;

	PG_RETURN_POINTER(state1);
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
	Numeric	sum_x, sum_x2, cnt;
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

		if (i >= from)
		{
			sum_x = add_numeric(sum_x, (Numeric)ptr);
			sum_x2 = add_numeric(
						sum_x2,
						mul_numeric((Numeric)ptr, (Numeric)ptr));
		}
	}

	sum_x2 = mul_numeric(cnt, sum_x2);
	sum_x = mul_numeric(sum_x, sum_x);

	/* Watch out for roundoff error producing a negative numerator */
	if (numeric_comparator(&sum_x2, &sum_x) <= 0)
		PG_RETURN_NUMERIC(create_numeric(0));

	PG_RETURN_NUMERIC (sub_numeric(
							div_numeric(
								sum_x2,
								mul_numeric(cnt, cnt)),
							div_numeric(
								sum_x,
								mul_numeric(cnt, cnt))));
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
	Numeric	sum_x, sum_x2, cnt;
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

		if (i >= from)
		{
			sum_x = add_numeric(sum_x, (Numeric)ptr);
			sum_x2 = add_numeric(
							sum_x2,
							mul_numeric((Numeric)ptr, (Numeric)ptr));
		}
	}

	/* Watch out for roundoff error producing a negative numerator */
	sum_x2 = mul_numeric(cnt, sum_x2);
	sum_x = mul_numeric(sum_x, sum_x);

	if (numeric_comparator(&sum_x2, &sum_x) <= 0)
		PG_RETURN_NUMERIC(create_numeric(0));

	PG_RETURN_NUMERIC (sub_numeric(
							div_numeric(
								sum_x2,
								mul_numeric(
									cnt,
									sub_numeric(cnt, create_numeric(1)))),
							div_numeric(
								sum_x,
								mul_numeric(
									cnt,
									sub_numeric(cnt, create_numeric(1))))));
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

	PG_RETURN_FLOAT8 (sqrt(result/cnt));
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

	PG_RETURN_FLOAT8 (sqrt(result/cnt));
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

	PG_RETURN_FLOAT8 (sqrt(result/cnt));
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
	Numeric	sum_x, sum_x2, cnt;
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

		if (i >= from)
		{
			sum_x = add_numeric(sum_x, (Numeric)ptr);
			sum_x2 = add_numeric(sum_x2,
								 mul_numeric((Numeric)ptr, (Numeric)ptr));
		}
	}

	/* Watch out for roundoff error producing a negative numerator */
	sum_x2 = mul_numeric(cnt, sum_x2);
	sum_x = mul_numeric(sum_x, sum_x);

	if (numeric_comparator(&sum_x2, &sum_x) <= 0)
		PG_RETURN_NUMERIC(create_numeric(0));

	PG_RETURN_NUMERIC (sqrt_numeric(
							sub_numeric(
								div_numeric(
									sum_x2,
									pow_numeric(cnt, 2)),
								div_numeric(
									sum_x,
									pow_numeric(cnt, 2)))));
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
	Numeric	sum_x, sum_x2, cnt;
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

		if (i >= from)
		{
			sum_x = add_numeric(sum_x, (Numeric)ptr);
			sum_x2 = add_numeric(sum_x2, pow_numeric((Numeric)ptr, 2));
		}
	}

	/* Watch out for roundoff error producing a negative numerator */
	sum_x2 = mul_numeric(cnt, sum_x2);
	sum_x = pow_numeric(sum_x, 2);

	if (numeric_comparator(&sum_x2, &sum_x) <= 0)
		PG_RETURN_NUMERIC(create_numeric(0));

	PG_RETURN_NUMERIC (sqrt_numeric(
							sub_numeric(
								div_numeric(
									sum_x2,
									mul_numeric(
										cnt,
										sub_numeric(cnt, create_numeric(1)))),
								div_numeric(
									sum_x,
									mul_numeric(
										cnt,
										sub_numeric(cnt, create_numeric(1)))))));
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
	return DatumGetInt32(
			DirectFunctionCall2(numeric_cmp,
								NumericGetDatum(* (Numeric *) a),
								NumericGetDatum(* (Numeric *) b)));
}

static Numeric
create_numeric(int value)
{
	return DatumGetNumeric(
			DirectFunctionCall1(int4_numeric,
								Int32GetDatum(value)));
}

static Numeric
add_numeric(Numeric a, Numeric b)
{
	return DatumGetNumeric(
			DirectFunctionCall2(numeric_add,
								NumericGetDatum(a),
								NumericGetDatum(b)));
}

static Numeric
div_numeric(Numeric a, Numeric b)
{
	return DatumGetNumeric(
			DirectFunctionCall2(numeric_div,
								NumericGetDatum(a),
								NumericGetDatum(b)));
}

static Numeric
mul_numeric(Numeric a, Numeric b)
{
	return DatumGetNumeric(
			DirectFunctionCall2(numeric_mul,
								NumericGetDatum(a),
								NumericGetDatum(b)));
}

static Numeric
sub_numeric(Numeric a, Numeric b)
{
	return DatumGetNumeric(
			DirectFunctionCall2(numeric_sub,
								NumericGetDatum(a),
								NumericGetDatum(b)));
}

static Numeric
pow_numeric(Numeric a, int b)
{
	return DatumGetNumeric(
			DirectFunctionCall2(numeric_power,
								NumericGetDatum(a),
								NumericGetDatum(create_numeric(b))));
}

static Numeric
sqrt_numeric(Numeric a)
{
	return DatumGetNumeric(
			DirectFunctionCall1(numeric_sqrt,
								NumericGetDatum(a)));
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

	return makeArrayResult(astate, CurrentMemoryContext);
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

	return makeArrayResult(astate, CurrentMemoryContext);
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
