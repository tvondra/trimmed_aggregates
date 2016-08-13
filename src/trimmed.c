/*
* trimmed.c - Trimmed aggregate functions
* Copyright (C) Tomas Vondra, 2011-2016
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

	double *elements;		/* array of values */
} state_double;

typedef struct state_int32
{
	int		maxelements;	/* size of elements array */
	int		nelements;		/* number of used items */

	double	cut_lower;		/* fraction to cut at the lower end */
	double	cut_upper;		/* fraction to cut at the upper end */

	int32  *elements;		/* array of values */
} state_int32;

typedef struct state_int64
{
	int		maxelements;	/* size of elements array */
	int		nelements;		/* number of used items */

	double	cut_lower;		/* fraction to cut at the lower end */
	double	cut_upper;		/* fraction to cut at the upper end */

	int64  *elements;		/* array of values */
} state_int64;

typedef struct state_numeric
{
	int		maxelements;	/* size of elements array */
	int		nelements;		/* number of used items */

	double	cut_lower;		/* fraction to cut at the lower end */
	double	cut_upper;		/* fraction to cut at the upper end */

	Numeric *elements;		/* array of values */
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


/* These functions use a bit dirty trick to pass the data - the int
 * value is actually a pointer to the array allocated in the parent
 * memory context. A bit ugly but works fine.
 *
 * The memory consumption might be a problem, as all the values are
 * kept in the memory - for example 1.000.000 of 8-byte values (bigint)
 * requires about 8MB of memory.
 */

Datum
trimmed_append_double(PG_FUNCTION_ARGS)
{
	state_double *data;
	MemoryContext aggcontext;

	GET_AGG_CONTEXT("trimmed_append_double", fcinfo, aggcontext);

	if (PG_ARGISNULL(0))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(aggcontext);

		data = (state_double*)palloc(sizeof(state_double));
		data->elements = (double*)palloc(MIN_ELEMENTS * sizeof(double));

		MemoryContextSwitchTo(oldcontext);

		data->maxelements = MIN_ELEMENTS;
		data->nelements = 0;

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

	if (PG_ARGISNULL(0))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(aggcontext);

		data = (state_int32*)palloc(sizeof(state_int32));
		data->elements = (int32*)palloc(MIN_ELEMENTS * sizeof(int32));

		MemoryContextSwitchTo(oldcontext);

		data->maxelements = MIN_ELEMENTS;
		data->nelements = 0;

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

	if (PG_ARGISNULL(0))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(aggcontext);

		data = (state_int64*)palloc(sizeof(state_int64));
		data->elements = (int64*)palloc(MIN_ELEMENTS * sizeof(int64));

		MemoryContextSwitchTo(oldcontext);

		data->maxelements = MIN_ELEMENTS;
		data->nelements = 0;

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

	if (PG_ARGISNULL(0))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(aggcontext);

		data = (state_numeric*)palloc(sizeof(state_numeric));
		data->elements = (Numeric*)palloc(MIN_ELEMENTS * sizeof(Numeric));

		MemoryContextSwitchTo(oldcontext);

		data->maxelements = MIN_ELEMENTS;
		data->nelements = 0;

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
		MemoryContext oldcontext;
		Numeric element = PG_GETARG_NUMERIC(1);

		if (data->nelements >= data->maxelements)
		{
			data->maxelements *= 2;
			data->elements = (Numeric*)repalloc(data->elements,
												sizeof(Numeric) * data->maxelements);
		}

		oldcontext = MemoryContextSwitchTo(aggcontext);

		data->elements[data->nelements++]
			= DatumGetNumeric(datumCopy(NumericGetDatum(element), false, -1));

		MemoryContextSwitchTo(oldcontext);
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

	memcpy(ptr, data, offsetof(state_int64, elements));
	ptr += offsetof(state_int64, elements);

	memcpy(ptr, data->elements, len);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_serial_numeric(PG_FUNCTION_ARGS)
{
	int				i;
	Size			hlen = offsetof(state_numeric, elements);	/* header */
	Size			len;										/* elements */
	state_numeric *data = (state_numeric *)PG_GETARG_POINTER(0);
	bytea		   *out;
	char		   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_numeric", fcinfo);

	/* sum sizes of all Numeric values to get the required size */
	len = 0;
	for (i = 0; i < data->nelements; i++)
		len += VARSIZE(data->elements[i]);

	out = (bytea *)palloc(VARHDRSZ + len + hlen);
	SET_VARSIZE(out, VARHDRSZ + len + hlen);

	ptr = (char*) VARDATA(out);

	memcpy(ptr, data, offsetof(state_numeric, elements));
	ptr += offsetof(state_numeric, elements);

	/* now copy the contents of each Numeric value into the buffer */
	for (i = 0; i < data->nelements; i++)
	{
		memcpy(ptr, data->elements[i], VARSIZE(data->elements[i]));
		ptr += VARSIZE(data->elements[i]);
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
	int		i;
	state_numeric *out = (state_numeric *)palloc(sizeof(state_numeric));
	bytea  *data = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(data);
	char   *ptr = VARDATA(data);
	char   *tmp;

	CHECK_AGG_CONTEXT("trimmed_deserial_numeric", fcinfo);

	Assert(len > 0);

	/* first read the struct header, stored at the beginning */
	memcpy(out, ptr, offsetof(state_numeric, elements));
	ptr += offsetof(state_numeric, elements);

	/* allocate an array with enough space for the Numeric pointers */
	out->maxelements = out->nelements; /* no slack space for new data */
	out->elements = (Numeric *)palloc(out->nelements * sizeof(Numeric));

	/*
	 * we also need to copy the Numeric contents, but instead of copying
	 * the values one by one, we copy the chunk of the serialized data
	 */
	tmp = palloc(len - sizeof(int));
	memcpy(tmp, ptr, len - sizeof(int));
	ptr = tmp;

	/* and now just set the pointers in the elements array */
	for (i = 0; i < out->nelements; i++)
	{
		out->elements[i] = (Numeric)ptr;
		ptr += VARSIZE(ptr);

		Assert(ptr <= tmp + (len - sizeof(int)));
	}

	PG_RETURN_POINTER(out);
}

Datum
trimmed_combine_double(PG_FUNCTION_ARGS)
{
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

		data1->elements = (double*)palloc(sizeof(double) * data2->maxelements);

		memcpy(data1->elements, data2->elements, sizeof(double) * data2->maxelements);

		MemoryContextSwitchTo(old_context);

		/* free the internal state */
		pfree(data2->elements);
		data2->elements = NULL;

		PG_RETURN_POINTER(data1);
	}

	Assert((data1 != NULL) && (data2 != NULL));

	/* if there's not enough space in data1, enlarge it */
	if (data1->nelements + data2->nelements >= data1->maxelements)
	{
		/* we size the array to match the size exactly */
		data1->maxelements = data1->nelements + data2->nelements;
		data1->elements = (double *)repalloc(data1->elements,
											 data1->maxelements * sizeof(double));
	}

	/* copy the elements from data2 into data1 */
	memcpy(data1->elements + data1->nelements, data2->elements,
		   data2->nelements * sizeof(double));

	/* and finally remember the current number of elements */
	data1->nelements += data2->nelements;

	PG_RETURN_POINTER(data1);
}

Datum
trimmed_combine_int32(PG_FUNCTION_ARGS)
{
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

	/* if there's not enough space in data1, enlarge it */
	if (data1->nelements + data2->nelements >= data1->maxelements)
	{
		/* we size the array to match the size exactly */
		data1->maxelements = data1->nelements + data2->nelements;
		data1->elements = (int32 *)repalloc(data1->elements,
											 data1->maxelements * sizeof(int32));
	}

	/* copy the elements from data2 into data1 */
	memcpy(data1->elements + data1->nelements, data2->elements,
		   data2->nelements * sizeof(int32));

	/* and finally remember the current number of elements */
	data1->nelements += data2->nelements;

	PG_RETURN_POINTER(data1);
}

Datum
trimmed_combine_int64(PG_FUNCTION_ARGS)
{
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

		PG_RETURN_POINTER(data1);
	}

	/* if there's not enough space in data1, enlarge it */
	if (data1->nelements + data2->nelements >= data1->maxelements)
	{
		/* we size the array to match the size exactly */
		data1->maxelements = data1->nelements + data2->nelements;
		data1->elements = (int64 *)repalloc(data1->elements,
											 data1->maxelements * sizeof(int64));
	}

	/* copy the elements from data2 into data1 */
	memcpy(data1->elements + data1->nelements, data2->elements,
		   data2->nelements * sizeof(int64));

	/* and finally remember the current number of elements */
	data1->nelements += data2->nelements;

	PG_RETURN_POINTER(data1);
}

Datum
trimmed_combine_numeric(PG_FUNCTION_ARGS)
{
	Size			len;
	int				i;
	state_numeric *data1;
	state_numeric *data2;
	MemoryContext agg_context;
	MemoryContext old_context;
	char		   *tmp;

	GET_AGG_CONTEXT("trimmed_combine_numeric", fcinfo, agg_context);

	data1 = PG_ARGISNULL(0) ? NULL : (state_numeric *) PG_GETARG_POINTER(0);
	data2 = PG_ARGISNULL(1) ? NULL : (state_numeric *) PG_GETARG_POINTER(1);

	if (data2 == NULL)
		PG_RETURN_POINTER(data1);

	if (data1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		data1 = (state_numeric *)palloc(sizeof(state_numeric));
		data1->maxelements = data2->maxelements;
		data1->nelements = 0;

		data1->cut_lower = data2->cut_lower;
		data1->cut_upper = data2->cut_upper;

		data1->elements = (Numeric*)palloc(sizeof(Numeric) * data2->maxelements);

		len = 0;
		for (i = 0; i < data2->nelements; i++)
			len += VARSIZE(data2->elements[i]);

		tmp = palloc(len);

		for (i = 0; i < data2->nelements; i++)
		{
			memcpy(tmp, data2->elements[i], VARSIZE(data2->elements[i]));
			data1->elements[data1->nelements++] = (Numeric)tmp;
			tmp += VARSIZE(data2->elements[i]);
		}

		MemoryContextSwitchTo(old_context);

		/* free the internal state */
		pfree(data2->elements);
		data2->elements = NULL;

		PG_RETURN_POINTER(data1);
	}

	/* if there's not enough space in data1, enlarge it */
	if (data1->nelements + data2->nelements >= data1->maxelements)
	{
		/* we size the array to match the size exactly */
		data1->maxelements = data1->nelements + data2->nelements;
		data1->elements = (Numeric *)repalloc(data1->elements,
											  data1->maxelements * sizeof(Numeric));
	}

	/*
	 * we can't copy just the pointers - we need to copy the contents of the
	 * Numeric datums too - to save space, we'll copy them into a single buffer
	 * and use the pointers
	 */
	len = 0;
	for (i = 0; i < data2->nelements; i++)
		len += VARSIZE(data2->elements[i]);

	old_context = MemoryContextSwitchTo(agg_context);
	tmp = palloc(len);
	MemoryContextSwitchTo(old_context);

	for (i = 0; i < data2->nelements; i++)
	{
		memcpy(tmp, data2->elements[i], VARSIZE(data2->elements[i]));
		data1->elements[data1->nelements++] = (Numeric)tmp;
		tmp += VARSIZE(data2->elements[i]);
	}

	Assert(data1->nelements == data1->maxelements);

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

	pg_qsort(data->elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
		result = add_numeric(result, div_numeric(data->elements[i], cnt));

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

	pg_qsort(data->elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	/* average */
	result[0] = create_numeric(0);
	result[1] = create_numeric(0);
	result[2] = create_numeric(0);

	sum_x   = create_numeric(0);
	sum_x2  = create_numeric(0);

	/* compute sumX and sumX2 */
	for (i = from; i < to; i++)
	{
		sum_x  = add_numeric(sum_x, data->elements[i]);
		sum_x2 = add_numeric(sum_x2,
							 mul_numeric(data->elements[i],
										 data->elements[i]));
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
		Numeric	 delta = sub_numeric(data->elements[i], result[0]);
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

	pg_qsort(data->elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
		avg = add_numeric(avg, div_numeric(data->elements[i], cnt));

	for (i = from; i < to; i++)
		result = add_numeric(
					result,
					div_numeric(
						pow_numeric(sub_numeric(data->elements[i],avg),2),
						cnt));

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

	pg_qsort(data->elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = add_numeric(sum_x, data->elements[i]);
		sum_x2 = add_numeric(
					sum_x2,
					mul_numeric(data->elements[i], data->elements[i]));
	}

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

	pg_qsort(data->elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = add_numeric(sum_x, data->elements[i]);
		sum_x2 = add_numeric(
						sum_x2,
						mul_numeric(data->elements[i], data->elements[i]));
	}

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

	pg_qsort(data->elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
		avg = add_numeric(avg, div_numeric(data->elements[i], cnt));

	for (i = from; i < to; i++)
		result = add_numeric(
					result,
					div_numeric(
						pow_numeric(sub_numeric(data->elements[i], avg), 2),
						cnt));

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

	pg_qsort(data->elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = add_numeric(sum_x, data->elements[i]);
		sum_x2 = add_numeric(sum_x2,
							 mul_numeric(data->elements[i],
										 data->elements[i]));
	}

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

	pg_qsort(data->elements, data->nelements, sizeof(Numeric), &numeric_comparator);

	for (i = from; i < to; i++)
	{
		sum_x = add_numeric(sum_x, data->elements[i]);
		sum_x2 = add_numeric(sum_x2, pow_numeric(data->elements[i], 2));
	}

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
