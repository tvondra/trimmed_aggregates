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

static Datum
double_to_array(FunctionCallInfo fcinfo, double * d, int len);

static Datum
numeric_to_array(FunctionCallInfo fcinfo, Numeric * d, int len);

static Numeric *
build_numeric_elements(state_numeric *state);

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
	state_double *data;
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

		data = (state_double*)palloc(sizeof(state_double));
		data->elements = (double*)palloc(MIN_ELEMENTS * sizeof(double));

		MemoryContextSwitchTo(oldcontext);

		data->maxelements = MIN_ELEMENTS;
		data->nelements = 0;
		data->sorted = false;

		/* how much to cut */
		if (PG_ARGISNULL(2) || PG_ARGISNULL(3))
			elog(ERROR, "both upper and lower cut must not be NULL");

		data->cut_lower = PG_GETARG_FLOAT8(2);
		data->cut_upper = PG_GETARG_FLOAT8(3);

		if (data->cut_lower < 0.0 || data->cut_lower >= 1.0)
			elog(ERROR, "lower cut needs to be between 0 and 1 (inclusive)");

		if (data->cut_upper < 0.0 || data->cut_upper >= 1.0)
			elog(ERROR, "upper cut needs to be between 0 and 1 (inclusive)");

		if (data->cut_lower + data->cut_upper >= 1.0)
			elog(ERROR, "lower and upper cut sum to >= 1.0");
	}
	else
		data = (state_double*)PG_GETARG_POINTER(0);

	if (! PG_ARGISNULL(1))
	{
		double element = PG_GETARG_FLOAT8(1);

		if (data->nelements >= data->maxelements)
		{
			data->maxelements *= 2;
			data->elements = (double*)repalloc(data->elements,
											   sizeof(double) * data->maxelements);
		}

		data->elements[data->nelements++] = element;
	}

	PG_RETURN_POINTER(data);
}

Datum
trimmed_append_int32(PG_FUNCTION_ARGS)
{
	state_int32 *data;
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

		data = (state_int32*)palloc(sizeof(state_int32));
		data->elements = (int32*)palloc(MIN_ELEMENTS * sizeof(int32));

		MemoryContextSwitchTo(oldcontext);

		data->maxelements = MIN_ELEMENTS;
		data->nelements = 0;
		data->sorted = false;

		/* how much to cut */
		if (PG_ARGISNULL(2) || PG_ARGISNULL(3))
			elog(ERROR, "both upper and lower cut must not be NULL");

		data->cut_lower = PG_GETARG_FLOAT8(2);
		data->cut_upper = PG_GETARG_FLOAT8(3);

		if (data->cut_lower < 0.0 || data->cut_lower >= 1.0)
			elog(ERROR, "lower cut needs to be between 0 and 1 (inclusive)");

		if (data->cut_upper < 0.0 || data->cut_upper >= 1.0)
			elog(ERROR, "upper cut needs to be between 0 and 1 (inclusive)");

		if (data->cut_lower + data->cut_upper >= 1.0)
			elog(ERROR, "lower and upper cut sum to >= 1.0");
	}
	else
		data = (state_int32*)PG_GETARG_POINTER(0);

	if (! PG_ARGISNULL(1))
	{
		int32 element = PG_GETARG_INT32(1);

		if (data->nelements >= data->maxelements)
		{
			data->maxelements *= 2;
			data->elements = (int32*)repalloc(data->elements,
											  sizeof(int32) * data->maxelements);
		}

		data->elements[data->nelements++] = element;
	}

	PG_RETURN_POINTER(data);
}

Datum
trimmed_append_int64(PG_FUNCTION_ARGS)
{
	state_int64 *data;
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

		data = (state_int64*)palloc(sizeof(state_int64));
		data->elements = (int64*)palloc(MIN_ELEMENTS * sizeof(int64));

		MemoryContextSwitchTo(oldcontext);

		data->maxelements = MIN_ELEMENTS;
		data->nelements = 0;
		data->sorted = false;

		/* how much to cut */
		if (PG_ARGISNULL(2) || PG_ARGISNULL(3))
			elog(ERROR, "both upper and lower cut must not be NULL");

		data->cut_lower = PG_GETARG_FLOAT8(2);
		data->cut_upper = PG_GETARG_FLOAT8(3);

		if (data->cut_lower < 0.0 || data->cut_lower >= 1.0)
			elog(ERROR, "lower cut needs to be between 0 and 1 (inclusive)");

		if (data->cut_upper < 0.0 || data->cut_upper >= 1.0)
			elog(ERROR, "upper cut needs to be between 0 and 1 (inclusive)");

		if (data->cut_lower + data->cut_upper >= 1.0)
			elog(ERROR, "lower and upper cut sum to >= 1.0");
	}
	else
		data = (state_int64*)PG_GETARG_POINTER(0);

	if (! PG_ARGISNULL(1))
	{
		int64 element = PG_GETARG_INT64(1);

		if (data->nelements >= data->maxelements)
		{
			data->maxelements *= 2;
			data->elements = (int64*)repalloc(data->elements,
											  sizeof(int64) * data->maxelements);
		}

		data->elements[data->nelements++] = element;
	}

	PG_RETURN_POINTER(data);
}

Datum
trimmed_append_numeric(PG_FUNCTION_ARGS)
{
	state_numeric *data;
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
		data = (state_numeric*)MemoryContextAlloc(aggcontext,
												  sizeof(state_numeric));

		data->nelements = 0;
		data->data = NULL;
		data->usedlen = 0;
		data->maxlen = 32;	/* TODO make this a constant */

		/* how much to cut */
		if (PG_ARGISNULL(2) || PG_ARGISNULL(3))
			elog(ERROR, "both upper and lower cut must not be NULL");

		data->cut_lower = PG_GETARG_FLOAT8(2);
		data->cut_upper = PG_GETARG_FLOAT8(3);

		if (data->cut_lower < 0.0 || data->cut_lower >= 1.0)
			elog(ERROR, "lower cut needs to be between 0 and 1 (inclusive)");

		if (data->cut_upper < 0.0 || data->cut_upper >= 1.0)
			elog(ERROR, "upper cut needs to be between 0 and 1 (inclusive)");

		if (data->cut_lower + data->cut_upper >= 1.0)
			elog(ERROR, "lower and upper cut sum to >= 1.0");
	}
	else
		data = (state_numeric*)PG_GETARG_POINTER(0);

	if (! PG_ARGISNULL(1))
	{
		Numeric element = PG_GETARG_NUMERIC(1);

		int len = VARSIZE(element);

		/* if there's not enough space in the data buffer, repalloc it */
		if (data->usedlen + len > data->maxlen)
		{
			while (len + data->usedlen > data->maxlen)
				data->maxlen *= 2;

			if (data->data != NULL)
				data->data = repalloc(data->data, data->maxlen);
		}

		/* if first entry, we need to allocate the buffer */
		if (! data->data)
			data->data = MemoryContextAlloc(aggcontext, data->maxlen);

		/* copy the contents of the Numeric in place */
		memcpy(data->data + data->usedlen, element, len);

		data->usedlen += len;
		data->nelements += 1;
	}

	PG_RETURN_POINTER(data);
}

Datum
trimmed_serial_double(PG_FUNCTION_ARGS)
{
	state_double  *data = (state_double *)PG_GETARG_POINTER(0);
	Size			hlen = offsetof(state_double, elements);	/* header */
	Size			len = data->nelements * sizeof(double);			/* elements */
	bytea		   *out = (bytea *)palloc(VARHDRSZ + len + hlen);
	char		   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_double", fcinfo);

	SET_VARSIZE(out, VARHDRSZ + len + hlen);

	ptr = VARDATA(out);

	/* we want to serialize the data in sorted format */
	if (! data->sorted)
	{
		pg_qsort(data->elements, data->nelements, sizeof(double), &double_comparator);
		data->sorted = true;
	}

	memcpy(ptr, data, offsetof(state_double, elements));
	ptr += offsetof(state_double, elements);

	memcpy(ptr, data->elements, len);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_serial_int32(PG_FUNCTION_ARGS)
{
	state_int32   *data = (state_int32 *)PG_GETARG_POINTER(0);
	Size			hlen = offsetof(state_int32, elements);	/* header */
	Size			len = data->nelements * sizeof(int32);			/* elements */
	bytea		   *out = (bytea *)palloc(VARHDRSZ + len + hlen);
	char		   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_int32", fcinfo);

	SET_VARSIZE(out, VARHDRSZ + len + hlen);

	ptr = VARDATA(out);

	/* we want to serialize the data in sorted format */
	if (! data->sorted)
	{
		pg_qsort(data->elements, data->nelements, sizeof(double), &int32_comparator);
		data->sorted = true;
	}

	memcpy(ptr, data, offsetof(state_int32, elements));
	ptr += offsetof(state_int32, elements);

	memcpy(ptr, data->elements, len);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_serial_int64(PG_FUNCTION_ARGS)
{
	state_int64  *data = (state_int64 *)PG_GETARG_POINTER(0);
	Size			hlen = offsetof(state_int64, elements);	/* header */
	Size			len = data->nelements * sizeof(int64);			/* elements */
	bytea		   *out = (bytea *)palloc(VARHDRSZ + len + hlen);
	char		   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_int64", fcinfo);

	SET_VARSIZE(out, VARHDRSZ + len + hlen);

	ptr = VARDATA(out);

	/* we want to serialize the data in sorted format */
	if (! data->sorted)
	{
		pg_qsort(data->elements, data->nelements, sizeof(double), &int64_comparator);
		data->sorted = true;
	}

	memcpy(ptr, data, offsetof(state_int64, elements));
	ptr += offsetof(state_int64, elements);

	memcpy(ptr, data->elements, len);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_serial_numeric(PG_FUNCTION_ARGS)
{
	state_numeric *data = (state_numeric *)PG_GETARG_POINTER(0);
	Size	hlen = offsetof(state_numeric, data);		/* header */
	Size	len = data->usedlen;						/* elements */
	bytea  *out;
	char   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_numeric", fcinfo);

	out = (bytea *)palloc(VARHDRSZ + len + hlen);
	SET_VARSIZE(out, VARHDRSZ + len + hlen);

	ptr = (char*) VARDATA(out);

	if (! data->sorted)	/* not sorted yet */
	{
		int		i;
		char   *tmp = data->data;
		Numeric *items = (Numeric*)palloc(sizeof(Numeric) * data->nelements);

		i = 0;
		while (tmp < data->data + data->usedlen)
		{
			items[i++] = (Numeric)tmp;
			tmp += VARSIZE(tmp);
		}

		Assert(i == data->nelements);

		pg_qsort(items, data->nelements, sizeof(Numeric), &numeric_comparator);

		data->sorted = true;

		memcpy(ptr, data, offsetof(state_numeric, data));
		ptr += offsetof(state_numeric, data);

		data->sorted = false; /* reset the state back (not sorting in-place) */

		/* copy the values in place */
		for (i = 0; i < data->nelements; i++)
		{
			memcpy(ptr, items[i], VARSIZE(items[i]));
			ptr += VARSIZE(items[i]);
		}

		pfree(items);
	}
	else	/* already sorted, copy as a chunk at once */
	{
		memcpy(ptr, data, offsetof(state_numeric, data));
		ptr += offsetof(state_numeric, data);

		memcpy(ptr, data->data, data->usedlen);
		ptr += data->usedlen;
	}

	/* we better get exactly the expected amount of data */
	Assert((char*)VARDATA(out) + len + hlen == ptr);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_deserial_double(PG_FUNCTION_ARGS)
{
	state_double *out = (state_double *)palloc(sizeof(state_double));
	bytea  *data = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(data);
	char   *ptr = VARDATA(data);

	CHECK_AGG_CONTEXT("trimmed_deserial_double", fcinfo);

	Assert(len > 0);
	Assert((len - offsetof(state_double, elements)) % sizeof(double) == 0);

	/* copy the header */
	memcpy(out, ptr, offsetof(state_double, elements));
	ptr += offsetof(state_double, elements);

	Assert(len == offsetof(state_double, elements) + out->nelements * sizeof(double));

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
	bytea  *data = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(data);
	char   *ptr = VARDATA(data);

	CHECK_AGG_CONTEXT("trimmed_deserial_int32", fcinfo);

	Assert(len > 0);
	Assert((len - offsetof(state_int32, elements)) % sizeof(int32) == 0);

	/* copy the header */
	memcpy(out, ptr, offsetof(state_int32, elements));
	ptr += offsetof(state_int32, elements);

	Assert(len == offsetof(state_int32, elements) + out->nelements * sizeof(int32));

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
	bytea  *data = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(data);
	char   *ptr = VARDATA(data);

	CHECK_AGG_CONTEXT("trimmed_deserial_int64", fcinfo);

	Assert(len > 0);
	Assert((len - offsetof(state_int64, elements)) % sizeof(int32) == 0);

	/* copy the header */
	memcpy(out, ptr, offsetof(state_int64, elements));
	ptr += offsetof(state_int64, elements);

	Assert(len == offsetof(state_int64, elements) + out->nelements * sizeof(int64));

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
	bytea  *data = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(data);
	char   *ptr = VARDATA(data);

	CHECK_AGG_CONTEXT("trimmed_deserial_numeric", fcinfo);

	Assert(len > 0);

	/* first read the struct header, stored at the beginning */
	memcpy(out, ptr, offsetof(state_numeric, data));
	ptr += offsetof(state_numeric, data);

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
	state_double *data1;
	state_double *data2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_double", fcinfo, agg_context);

	data1 = PG_ARGISNULL(0) ? NULL : (state_double *) PG_GETARG_POINTER(0);
	data2 = PG_ARGISNULL(1) ? NULL : (state_double *) PG_GETARG_POINTER(1);

	if (data2 == NULL)
		PG_RETURN_POINTER(data1);

	if (data1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		data1 = (state_double *)palloc(sizeof(state_double));
		data1->maxelements = data2->maxelements;
		data1->nelements = data2->nelements;

		data1->cut_lower = data2->cut_lower;
		data1->cut_upper = data2->cut_upper;
		data1->sorted = data2->sorted;

		data1->elements = (double*)palloc(sizeof(double) * data2->maxelements);

		memcpy(data1->elements, data2->elements, sizeof(double) * data2->maxelements);

		MemoryContextSwitchTo(old_context);

		/* free the internal state */
		pfree(data2->elements);
		data2->elements = NULL;

		PG_RETURN_POINTER(data1);
	}

	Assert((data1 != NULL) && (data2 != NULL));
	Assert(data1->sorted && data2->sorted);

	tmp = (double*)MemoryContextAlloc(agg_context,
					  sizeof(double) * (data1->nelements + data2->nelements));

	/* merge the two arrays */
	i = j = k = 0;
	while (true)
	{
		if ((i < data1->nelements) && (j < data2->nelements))
		{
			if (data1->elements[i] <= data2->elements[j])
				tmp[k++] = data1->elements[i++];
			else
				tmp[k++] = data2->elements[j++];
		}
		else if (i < data1->nelements)
			tmp[k++] = data1->elements[i++];
		else if (j < data2->nelements)
			tmp[k++] = data2->elements[j++];
		else
			/* no more elements to process */
			break;
	}

	/* free the two arrays */
	pfree(data1->elements);
	pfree(data2->elements);

	data1->elements = tmp;

	/* and finally remember the current number of elements */
	data1->nelements += data2->nelements;
	data1->maxelements = data1->nelements;

	PG_RETURN_POINTER(data1);
}

Datum
trimmed_combine_int32(PG_FUNCTION_ARGS)
{
	int i, j, k;
	int32 *tmp;
	state_int32 *data1;
	state_int32 *data2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_int32", fcinfo, agg_context);

	data1 = PG_ARGISNULL(0) ? NULL : (state_int32 *) PG_GETARG_POINTER(0);
	data2 = PG_ARGISNULL(1) ? NULL : (state_int32 *) PG_GETARG_POINTER(1);

	if (data2 == NULL)
		PG_RETURN_POINTER(data1);

	if (data1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		data1 = (state_int32 *)palloc(sizeof(state_int32));
		data1->maxelements = data2->maxelements;
		data1->nelements = data2->nelements;

		data1->cut_lower = data2->cut_lower;
		data1->cut_upper = data2->cut_upper;

		data1->elements = (int32*)palloc(sizeof(int32) * data2->maxelements);

		memcpy(data1->elements, data2->elements, sizeof(int32) * data2->maxelements);

		MemoryContextSwitchTo(old_context);

		/* free the internal state */
		pfree(data2->elements);
		data2->elements = NULL;

		PG_RETURN_POINTER(data1);
	}

	Assert((data1 != NULL) && (data2 != NULL));
	Assert(data1->sorted && data2->sorted);

	tmp = (int32*)MemoryContextAlloc(agg_context,
					  sizeof(int32) * (data1->nelements + data2->nelements));

	/* merge the two arrays */
	i = j = k = 0;
	while (true)
	{
		if ((i < data1->nelements) && (j < data2->nelements))
		{
			if (data1->elements[i] <= data2->elements[j])
				tmp[k++] = data1->elements[i++];
			else
				tmp[k++] = data2->elements[j++];
		}
		else if (i < data1->nelements)
			tmp[k++] = data1->elements[i++];
		else if (j < data2->nelements)
			tmp[k++] = data2->elements[j++];
		else
			/* no more elements to process */
			break;
	}

	/* free the two arrays */
	pfree(data1->elements);
	pfree(data2->elements);

	data1->elements = tmp;

	/* and finally remember the current number of elements */
	data1->nelements += data2->nelements;
	data1->maxelements = data1->nelements;

	PG_RETURN_POINTER(data1);
}

Datum
trimmed_combine_int64(PG_FUNCTION_ARGS)
{
	int i, j, k;
	int64 *tmp;
	state_int64 *data1;
	state_int64 *data2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_int64", fcinfo, agg_context);

	data1 = PG_ARGISNULL(0) ? NULL : (state_int64 *) PG_GETARG_POINTER(0);
	data2 = PG_ARGISNULL(1) ? NULL : (state_int64 *) PG_GETARG_POINTER(1);

	if (data2 == NULL)
		PG_RETURN_POINTER(data1);

	if (data1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		data1 = (state_int64 *)palloc(sizeof(state_int64));
		data1->maxelements = data2->maxelements;
		data1->nelements = data2->nelements;

		data1->cut_lower = data2->cut_lower;
		data1->cut_upper = data2->cut_upper;

		data1->elements = (int64*)palloc(sizeof(int64) * data2->maxelements);

		memcpy(data1->elements, data2->elements, sizeof(int64) * data2->maxelements);

		MemoryContextSwitchTo(old_context);

		/* free the internal state */
		pfree(data2->elements);
		data2->elements = NULL;

		PG_RETURN_POINTER(data1);
	}

	Assert((data1 != NULL) && (data2 != NULL));
	Assert(data1->sorted && data2->sorted);

	tmp = (int64*)MemoryContextAlloc(agg_context,
					  sizeof(int64) * (data1->nelements + data2->nelements));

	/* merge the two arrays */
	i = j = k = 0;
	while (true)
	{
		if ((i < data1->nelements) && (j < data2->nelements))
		{
			if (data1->elements[i] <= data2->elements[j])
				tmp[k++] = data1->elements[i++];
			else
				tmp[k++] = data2->elements[j++];
		}
		else if (i < data1->nelements)
			tmp[k++] = data1->elements[i++];
		else if (j < data2->nelements)
			tmp[k++] = data2->elements[j++];
		else
			/* no more elements to process */
			break;
	}

	/* free the two arrays */
	pfree(data1->elements);
	pfree(data2->elements);

	data1->elements = tmp;

	/* and finally remember the current number of elements */
	data1->nelements += data2->nelements;
	data1->maxelements = data1->nelements;

	PG_RETURN_POINTER(data1);
}

Datum
trimmed_combine_numeric(PG_FUNCTION_ARGS)
{
	int				i;
	state_numeric *data1;
	state_numeric *data2;
	MemoryContext agg_context;

	char		   *data, *tmp, *ptr1, *ptr2;

	GET_AGG_CONTEXT("trimmed_combine_numeric", fcinfo, agg_context);

	data1 = PG_ARGISNULL(0) ? NULL : (state_numeric *) PG_GETARG_POINTER(0);
	data2 = PG_ARGISNULL(1) ? NULL : (state_numeric *) PG_GETARG_POINTER(1);

	if (data2 == NULL)
		PG_RETURN_POINTER(data1);

	if (data1 == NULL)
	{
		data1 = (state_numeric *)MemoryContextAlloc(agg_context,
													sizeof(state_numeric));

		data1->nelements = data2->nelements;
		data1->cut_lower = data2->cut_lower;
		data1->cut_upper = data2->cut_upper;
		data1->usedlen = data2->usedlen;
		data1->maxlen = data2->maxlen;

		/* copy the buffer */
		data1->data = MemoryContextAlloc(agg_context, data1->usedlen);
		memcpy(data1->data, data2->data, data1->usedlen);

		PG_RETURN_POINTER(data1);
	}

	/* allocate temporary arrays */
	data = MemoryContextAlloc(agg_context, data1->usedlen + data2->usedlen);
	tmp = data;

	/* merge the two arrays */
	ptr1 = data1->data;
	ptr2 = data2->data;

	for (i = 0; i < data1->nelements + data2->nelements; i++)
	{
		Numeric element;

		Assert(ptr1 <= (data1->data + data1->usedlen));
		Assert(ptr2 <= (data2->data + data2->usedlen));

		if ((ptr1 < (data1->data + data1->usedlen)) &&
			(ptr2 < (data2->data + data2->usedlen)))
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
		else if (ptr1 < (data1->data + data1->usedlen))
		{
			element = (Numeric)ptr1;
			ptr1 += VARSIZE(ptr1);
		}
		else if (ptr2 < (data2->data + data2->usedlen))
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

	Assert(ptr1 == (data1->data + data1->usedlen));
	Assert(ptr2 == (data2->data + data2->usedlen));
	Assert((tmp - data) == (data1->usedlen + data2->usedlen));

	/* free the two arrays */
	pfree(data1->data);
	pfree(data2->data);

	data1->data = data;

	/* and finally remember the current number of elements */
	data1->nelements += data2->nelements;
	data1->usedlen += data2->usedlen;

	PG_RETURN_POINTER(data1);
}

Datum
trimmed_avg_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0;

	state_double *data;

	CHECK_AGG_CONTEXT("trimmed_avg_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_double*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(double), &double_comparator);

	for (i = from; i < to; i++)
		result = result + data->elements[i];

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_double_array(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	/* average, var_pop, var_samp, variance, stddev_pop, stddev_samp, stddev */
	double  result[7] = {0, 0, 0, 0, 0, 0, 0};

	state_double *data;

	CHECK_AGG_CONTEXT("trimmed_double_array", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_double*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(double), &double_comparator);

	/* average */
	result[0] = 0;
	result[1] = 1;
	result[2] = 2;

	for (i = from; i < to; i++)
	{
		result[0] += data->elements[i];
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	result[0] /= cnt;
	result[1] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt);	   /* var_pop */
	result[2] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)); /* var_samp */

	/* variance */
	result[3] = 0;
	for (i = from; i < to; i++)
		result[3] += (data->elements[i] - result[0]) * (data->elements[i] - result[0]);

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

	state_int32 *data;

	CHECK_AGG_CONTEXT("trimmed_avg_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int32), &int32_comparator);

	for (i = from; i < to; i++)
		result = result + (double)data->elements[i];

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_int32_array(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	/* average, var_pop, var_samp, variance, stddev_pop, stddev_samp, stddev */
	double	result[7] = {0, 0, 0, 0, 0, 0, 0};

	state_int32 *data;

	CHECK_AGG_CONTEXT("trimmed_int32_array", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int32), &int32_comparator);

	/* average */
	result[0] = 0;
	result[1] = 0;
	result[2] = 0;

	for (i = from; i < to; i++)
	{
		result[0] += (double)data->elements[i];
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + ((double)data->elements[i])*((double)data->elements[i]);
	}

	result[0] /= cnt;
	result[1] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt);	   /* var_pop */
	result[2] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)); /* var_samp */

	/* variance */
	result[3] = 0;
	for (i = from; i < to; i++)
		result[3] += ((double)data->elements[i] - result[0])*((double)data->elements[i] - result[0]);

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

	state_int64 *data;

	CHECK_AGG_CONTEXT("trimmed_avg_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int64), &int64_comparator);

	for (i = from; i < to; i++)
		result = result + (double)data->elements[i];

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_int64_array(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	/* average, var_pop, var_samp, variance, stddev_pop, stddev_samp, stddev */
	double	result[7] = {0, 0, 0, 0, 0, 0, 0};

	state_int64 *data;

	CHECK_AGG_CONTEXT("trimmed_int64_array", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int64), &int64_comparator);

	/* average */
	result[0] = 0;
	result[1] = 0;
	result[2] = 0;

	for (i = from; i < to; i++)
	{
		result[0] += (double)data->elements[i];
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + ((double)data->elements[i])*((double)data->elements[i]);
	}

	result[0] /= cnt;
	result[1] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt);	   /* var_pop */
	result[2] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)); /* var_samp */

	/* variance */
	result[3] = 0;
	for (i = from; i < to; i++)
		result[3] += ((double)data->elements[i] - result[0])*((double)data->elements[i] - result[0]);

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
	Numeric *elements;

	state_numeric *data;

	CHECK_AGG_CONTEXT("trimmed_avg_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);

	if (from >= to)
		PG_RETURN_NULL();

	/* create numeric values */
	cnt	= create_numeric(to-from);
	result = create_numeric(0);

	elements = build_numeric_elements(data);

	if (! data->sorted)
		pg_qsort(elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
		result = add_numeric(result, div_numeric(elements[i], cnt));

	/* free the temporary array */
	pfree(elements);

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
	Numeric *elements;

	state_numeric *data;

	CHECK_AGG_CONTEXT("trimmed_numeric_array", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);

	if (from >= to)
		PG_RETURN_NULL();

	/* create numeric values */
	cntNumeric = create_numeric(to-from);
	cntNumeric_1 = create_numeric(to-from-1);

	elements = build_numeric_elements(data);

	if (! data->sorted)
		pg_qsort(elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	/* average */
	result[0] = create_numeric(0);
	result[1] = create_numeric(0);
	result[2] = create_numeric(0);

	sum_x   = create_numeric(0);
	sum_x2  = create_numeric(0);

	/* compute sumX and sumX2 */
	for (i = from; i < to; i++)
	{
		sum_x  = add_numeric(sum_x, elements[i]);
		sum_x2 = add_numeric(sum_x2,
							 mul_numeric(elements[i],
										 elements[i]));
	}

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
	for (i = from; i < to; i++)
	{
		Numeric	 delta = sub_numeric(elements[i], result[0]);
		result[3]   = add_numeric(result[3], mul_numeric(delta, delta));
	}
	result[3] = div_numeric(result[3], cntNumeric);

	result[4] = sqrt_numeric(result[1]); /* stddev_pop */
	result[5] = sqrt_numeric(result[2]); /* stddev_samp */
	result[6] = sqrt_numeric(result[3]); /* stddev */

	/* free the temporary array */
	pfree(elements);

	return numeric_to_array(fcinfo, result, 7);
}

Datum
trimmed_var_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_double *data;

	CHECK_AGG_CONTEXT("trimmed_var_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_double*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(double), &double_comparator);

	for (i = from; i < to; i++)
		avg = avg + data->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (data->elements[i] - avg)*(data->elements[i] - avg);

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_var_int32(PG_FUNCTION_ARGS)
{

	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_int32 *data;

	CHECK_AGG_CONTEXT("trimmed_var_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int32), &int32_comparator);

	for (i = from; i < to; i++)
		avg = avg + (double)data->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (data->elements[i] - avg)*(data->elements[i] - avg);

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_var_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_int64 *data;

	CHECK_AGG_CONTEXT("trimmed_var_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int64), &int64_comparator);

	for (i = from; i < to; i++)
		avg = avg + (double)data->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (data->elements[i] - avg)*(data->elements[i] - avg);

	PG_RETURN_FLOAT8(result/cnt);
}

Datum
trimmed_var_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	result, avg, cnt;
	Numeric *elements;

	state_numeric *data;

	CHECK_AGG_CONTEXT("trimmed_var_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);

	if (from >= to)
		PG_RETURN_NULL();

	cnt = create_numeric(to - from);
	avg = create_numeric(0);
	result = create_numeric(0);

	elements = build_numeric_elements(data);

	if (! data->sorted)
		pg_qsort(elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
		avg = add_numeric(avg, div_numeric(elements[i], cnt));

	for (i = from; i < to; i++)
		result = add_numeric(
					result,
					div_numeric(
						pow_numeric(sub_numeric(elements[i],avg),2),
						cnt));

	pfree(elements);

	PG_RETURN_NUMERIC(result);
}

Datum
trimmed_var_pop_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	state_double *data;

	CHECK_AGG_CONTEXT("trimmed_var_pop_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_double*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	pg_qsort(data->elements, data->nelements, sizeof(double), &double_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt));
}

Datum
trimmed_var_pop_int32(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	state_int32 *data;

	CHECK_AGG_CONTEXT("trimmed_var_pop_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int32), &int32_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt));
}

Datum
trimmed_var_pop_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double  sum_x = 0, sum_x2 = 0;

	state_int64 *data;

	CHECK_AGG_CONTEXT("trimmed_var_pop_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int64), &int64_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt));
}

Datum
trimmed_var_pop_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	sum_x, sum_x2, cnt;
	Numeric *elements;

	state_numeric *data;

	CHECK_AGG_CONTEXT("trimmed_var_pop_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);

	if (from >= to)
		PG_RETURN_NULL();

	cnt = create_numeric(to - from);
	sum_x = create_numeric(0);
	sum_x2 = create_numeric(0);

	elements = build_numeric_elements(data);

	if (! data->sorted)
		pg_qsort(elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = add_numeric(sum_x, elements[i]);
		sum_x2 = add_numeric(
					sum_x2,
					mul_numeric(elements[i], elements[i]));
	}

	pfree(elements);

	PG_RETURN_NUMERIC (div_numeric(
							sub_numeric(
								mul_numeric(cnt, sum_x2),
								mul_numeric(sum_x, sum_x)),
							mul_numeric(cnt, cnt)));
}

Datum
trimmed_var_samp_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	state_double *data;

	CHECK_AGG_CONTEXT("trimmed_var_samp_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_double*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(double), &double_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)));
}

Datum
trimmed_var_samp_int32(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	state_int32 *data;

	CHECK_AGG_CONTEXT("trimmed_var_samp_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int32), &int32_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)));
}

Datum
trimmed_var_samp_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	state_int64 *data;

	CHECK_AGG_CONTEXT("trimmed_var_samp_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int64), &int64_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)));
}

Datum
trimmed_var_samp_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	sum_x, sum_x2, cnt;
	Numeric *elements;

	state_numeric *data;

	CHECK_AGG_CONTEXT("trimmed_var_samp_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);

	if (from >= to)
		PG_RETURN_NULL();

	cnt  = create_numeric(to - from);
	sum_x = create_numeric(0);
	sum_x2 = create_numeric(0);

	elements = build_numeric_elements(data);

	if (! data->sorted)
		pg_qsort(elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = add_numeric(sum_x, elements[i]);
		sum_x2 = add_numeric(
						sum_x2,
						mul_numeric(elements[i], elements[i]));
	}

	pfree(elements);

	PG_RETURN_NUMERIC (div_numeric(
							sub_numeric(
								mul_numeric(cnt, sum_x2),
								mul_numeric(sum_x, sum_x)
							),
							mul_numeric(
								cnt,
								sub_numeric(cnt, create_numeric(1)))));
}

Datum
trimmed_stddev_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_double *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_double*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(double), &double_comparator);

	for (i = from; i < to; i++)
		avg = avg + data->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (data->elements[i] - avg)*(data->elements[i] - avg);

	PG_RETURN_FLOAT8 (sqrt(result)/cnt);
}

Datum
trimmed_stddev_int32(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_int32 *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int32), &int32_comparator);

	for (i = from; i < to; i++)
		avg = avg + (double)data->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (data->elements[i] - avg)*(data->elements[i] - avg);

	PG_RETURN_FLOAT8 (sqrt(result)/cnt);
}

Datum
trimmed_stddev_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	result = 0, avg = 0;

	state_int64 *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int64), &int64_comparator);

	for (i = from; i < to; i++)
		avg = avg + (double)data->elements[i];
	avg /= cnt;

	for (i = from; i < to; i++)
		result = result + (data->elements[i] - avg)*(data->elements[i] - avg);

	PG_RETURN_FLOAT8 (sqrt(result)/cnt);
}

Datum
trimmed_stddev_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	result, avg, cnt;
	Numeric *elements;

	state_numeric *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);

	if (from >= to)
		PG_RETURN_NULL();

	cnt = create_numeric(to - from);
	avg = create_numeric(0);
	result = create_numeric(0);

	elements = build_numeric_elements(data);

	if (! data->sorted)
		pg_qsort(elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
		avg = add_numeric(avg, div_numeric(elements[i], cnt));

	for (i = from; i < to; i++)
		result = add_numeric(
					result,
					div_numeric(
						pow_numeric(sub_numeric(elements[i], avg), 2),
						cnt));

	pfree(elements);

	PG_RETURN_NUMERIC (sqrt_numeric(result));
}

Datum
trimmed_stddev_pop_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	state_double *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_pop_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_double*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(double), &double_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt)));
}

Datum
trimmed_stddev_pop_int32(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	state_int32 *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_pop_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int32), &int32_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt)));
}

Datum
trimmed_stddev_pop_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	state_int64 *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_pop_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int64), &int64_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt)));
}

Datum
trimmed_stddev_pop_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	sum_x, sum_x2, cnt;
	Numeric *elements;

	state_numeric *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_pop_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);

	if (from >= to)
		PG_RETURN_NULL();

	cnt  = create_numeric(to - from);
	sum_x = create_numeric(0);
	sum_x2 = create_numeric(0);

	elements = build_numeric_elements(data);

	if (! data->sorted)
		pg_qsort(elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = add_numeric(sum_x, elements[i]);
		sum_x2 = add_numeric(sum_x2,
							 mul_numeric(elements[i],
										 elements[i]));
	}

	pfree(elements);

	PG_RETURN_NUMERIC (sqrt_numeric(
							div_numeric(
								sub_numeric(
									mul_numeric(cnt, sum_x2),
									mul_numeric(sum_x, sum_x)),
								pow_numeric(cnt, 2))));
}

Datum
trimmed_stddev_samp_double(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	state_double *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_samp_double", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_double*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(double), &double_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1))));
}

Datum
trimmed_stddev_samp_int32(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	state_int32 *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_samp_int32", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int32*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int32), &int32_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1))));
}

Datum
trimmed_stddev_samp_int64(PG_FUNCTION_ARGS)
{
	int		i, from, to, cnt;
	double	sum_x = 0, sum_x2 = 0;

	state_int64 *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_samp_int64", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_int64*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);
	cnt  = (to - from);

	if (from >= to)
		PG_RETURN_NULL();

	if (! data->sorted)
		pg_qsort(data->elements, data->nelements, sizeof(int64), &int64_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = sum_x + data->elements[i];
		sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
	}

	PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1))));
}

Datum
trimmed_stddev_samp_numeric(PG_FUNCTION_ARGS)
{
	int		i, from, to;
	Numeric	sum_x, sum_x2, cnt;
	Numeric *elements;

	state_numeric *data;

	CHECK_AGG_CONTEXT("trimmed_stddev_samp_numeric", fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	data = (state_numeric*)PG_GETARG_POINTER(0);

	from = floor(data->nelements * data->cut_lower);
	to   = data->nelements - floor(data->nelements * data->cut_upper);

	if (from >= to)
		PG_RETURN_NULL();

	cnt  = create_numeric(to - from);
	sum_x = create_numeric(0);
	sum_x2 = create_numeric(0);

	elements = build_numeric_elements(data);

	if (! data->sorted)
		pg_qsort(elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = add_numeric(sum_x, elements[i]);
		sum_x2 = add_numeric(sum_x2, pow_numeric(elements[i], 2));
	}

	pfree(elements);

	PG_RETURN_NUMERIC (sqrt_numeric(
							div_numeric(
								sub_numeric(
									mul_numeric(cnt, sum_x2),
									pow_numeric(sum_x, 2)),
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

static Numeric *
build_numeric_elements(state_numeric *state)
{
	int		i;
	char   *tmp = state->data;
	Numeric *elements = (Numeric*)palloc(state->nelements * sizeof(Numeric));

	i = 0;
	while (tmp < state->data + state->usedlen)
	{
		elements[i++] = (Numeric)tmp;
		tmp += VARSIZE(tmp);

		Assert(i <= state->nelements);
	}

	Assert(i == state->nelements);

	return elements;
}
