/*
* quantile.c - Trimmed aggregate functions
* Copyright (C) Tomas Vondra, 2011
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
*
* TODO Take care of the memory consumption (respect work_mem, flush
* the data to disk if more space is needed and use merge sort, i.e.
* something like the ORDER BY uses).
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


#if (PG_VERSION_NUM >= 90000)

#define GET_AGG_CONTEXT(fname, fcinfo, aggcontext)  \
    if (! AggCheckCallContext(fcinfo, &aggcontext)) {   \
        elog(ERROR, "%s called in non-aggregate context", fname);  \
    }

#define CHECK_AGG_CONTEXT(fname, fcinfo)  \
    if (! AggCheckCallContext(fcinfo, NULL)) {   \
        elog(ERROR, "%s called in non-aggregate context", fname);  \
    }

#elif (PG_VERSION_NUM >= 80400)

#define GET_AGG_CONTEXT(fname, fcinfo, aggcontext)  \
    if (fcinfo->context && IsA(fcinfo->context, AggState)) {  \
        aggcontext = ((AggState *) fcinfo->context)->aggcontext;  \
    } else if (fcinfo->context && IsA(fcinfo->context, WindowAggState)) {  \
        aggcontext = ((WindowAggState *) fcinfo->context)->wincontext;  \
    } else {  \
        elog(ERROR, "%s called in non-aggregate context", fname);  \
        aggcontext = NULL;  \
    }

#define CHECK_AGG_CONTEXT(fname, fcinfo)  \
    if (!(fcinfo->context &&  \
        (IsA(fcinfo->context, AggState) ||  \
        IsA(fcinfo->context, WindowAggState))))  \
    {  \
        elog(ERROR, "%s called in non-aggregate context", fname);  \
    }

#endif

#define SLICE_SIZE 1024

/* FIXME The final functions copy a lot of code - refactor to share. */

/* Structures used to keep the data - the 'elements' array is extended
 * on the fly if needed. */

typedef struct struct_double {

    int nelements;
    int next;

    double cut_lower;
    double cut_upper;

    double * elements;

} struct_double;

typedef struct struct_int32 {

    int nelements;
    int next;

    double cut_lower;
    double cut_upper;

    int32  * elements;

} struct_int32;

typedef struct struct_int64 {

    int nelements;
    int next;

    double cut_lower;
    double cut_upper;

    int64  * elements;

} struct_int64;

typedef struct struct_numeric {

    int nelements;
    int next;

    double cut_lower;
    double cut_upper;

    Numeric * elements;

} struct_numeric;

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

    struct_double * data;

    double element;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    GET_AGG_CONTEXT("quantile_append_double", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0)) {

        data = (struct_double*)palloc(sizeof(struct_double));
        data->elements  = (double*)palloc(SLICE_SIZE*sizeof(double));
        data->nelements = SLICE_SIZE;
        data->next = 0;

        /* how much to cut */
        data->cut_lower = PG_GETARG_FLOAT8(2);
        data->cut_upper = PG_GETARG_FLOAT8(3);

    } else {
        data = (struct_double*)PG_GETARG_POINTER(0);
    }

    if (! PG_ARGISNULL(1)) {

        element = PG_GETARG_FLOAT8(1);

        if (data->next > data->nelements-1) {
            data->elements = (double*)repalloc(data->elements, sizeof(double)*(data->nelements + SLICE_SIZE));
            data->nelements = data->nelements + SLICE_SIZE;
        }

        data->elements[data->next++] = element;
    }

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(data);

}

Datum
trimmed_append_int32(PG_FUNCTION_ARGS)
{

    struct_int32 * data;

    int32 element;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    GET_AGG_CONTEXT("quantile_append_int32", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0)) {

        data = (struct_int32*)palloc(sizeof(struct_int32));
        data->elements  = (int32*)palloc(SLICE_SIZE*sizeof(int32));
        data->nelements = SLICE_SIZE;
        data->next = 0;

        /* how much to cut */
        data->cut_lower = PG_GETARG_FLOAT8(2);
        data->cut_upper = PG_GETARG_FLOAT8(3);

    } else {
        data = (struct_int32*)PG_GETARG_POINTER(0);
    }

    if (! PG_ARGISNULL(1)) {

        element = PG_GETARG_INT32(1);

        if (data->next > data->nelements-1) {
            data->elements = (int32*)repalloc(data->elements, sizeof(int32)*(data->nelements + SLICE_SIZE));
            data->nelements = data->nelements + SLICE_SIZE;
        }

        data->elements[data->next++] = element;

    }

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(data);

}

Datum
trimmed_append_int64(PG_FUNCTION_ARGS)
{

    struct_int64 * data;

    int64 element;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    GET_AGG_CONTEXT("quantile_append_64", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0)) {

        data = (struct_int64*)palloc(sizeof(struct_int64));
        data->elements  = (int64*)palloc(SLICE_SIZE*sizeof(int64));
        data->nelements = SLICE_SIZE;
        data->next = 0;

        /* how much to cut */
        data->cut_lower = PG_GETARG_FLOAT8(2);
        data->cut_upper = PG_GETARG_FLOAT8(3);

    } else {
        data = (struct_int64*)PG_GETARG_POINTER(0);
    }

    if (! PG_ARGISNULL(1)) {

        element = PG_GETARG_INT64(1);

        if (data->next > data->nelements-1) {
            data->elements = (int64*)repalloc(data->elements, sizeof(int64)*(data->nelements + SLICE_SIZE));
            data->nelements = data->nelements + SLICE_SIZE;
        }

        data->elements[data->next++] = element;

    }

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(data);

}

Datum
trimmed_append_numeric(PG_FUNCTION_ARGS)
{

    struct_numeric * data;

    Numeric element;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    GET_AGG_CONTEXT("trimmed_append_numeric", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0)) {

        data = (struct_numeric*)palloc(sizeof(struct_numeric));
        data->elements  = (Numeric*)palloc(SLICE_SIZE*sizeof(Numeric));
        data->nelements = SLICE_SIZE;
        data->next = 0;

        /* how much to cut */
        data->cut_lower = PG_GETARG_FLOAT8(2);
        data->cut_upper = PG_GETARG_FLOAT8(3);

    } else {
        data = (struct_numeric*)PG_GETARG_POINTER(0);
    }

    if (! PG_ARGISNULL(1)) {

        element = PG_GETARG_NUMERIC(1);

        if (data->next > data->nelements-1) {
            data->elements = (Numeric*)repalloc(data->elements, sizeof(Numeric)*(data->nelements + SLICE_SIZE));
            data->nelements = data->nelements + SLICE_SIZE;
        }

        data->elements[data->next++] = DatumGetNumeric(datumCopy(NumericGetDatum(element), false, -1));

    }

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(data);

}

Datum
trimmed_serial_double(PG_FUNCTION_ARGS)
{
	struct_double  *data = (struct_double *)PG_GETARG_POINTER(0);
	Size			hlen = offsetof(struct_double, elements);	/* header */
	Size			len = data->next * sizeof(double);			/* elements */
	bytea		   *out = (bytea *)palloc(VARHDRSZ + len) + hlen;
	char		   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_double", fcinfo);

	SET_VARSIZE(out, VARHDRSZ + len + hlen);

	ptr = VARDATA(out);

	memcpy(ptr, data, offsetof(struct_double, elements));
	ptr += offsetof(struct_double, elements);

	memcpy(ptr, data->elements, len);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_serial_int32(PG_FUNCTION_ARGS)
{
	struct_int32   *data = (struct_int32 *)PG_GETARG_POINTER(0);
	Size			hlen = offsetof(struct_int32, elements);	/* header */
	Size			len = data->next * sizeof(int32);			/* elements */
	bytea		   *out = (bytea *)palloc(VARHDRSZ + len + hlen);
	char		   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_int32", fcinfo);

	SET_VARSIZE(out, VARHDRSZ + len + hlen);

	ptr = VARDATA(out);

	memcpy(ptr, data, offsetof(struct_int32, elements));
	ptr += offsetof(struct_int32, elements);

	memcpy(ptr, data->elements, len);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_serial_int64(PG_FUNCTION_ARGS)
{
	struct_int64  *data = (struct_int64 *)PG_GETARG_POINTER(0);
	Size			hlen = offsetof(struct_int64, elements);	/* header */
	Size			len = data->next * sizeof(int64);			/* elements */
	bytea		   *out = (bytea *)palloc(VARHDRSZ + len + hlen);
	char		   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_int64", fcinfo);

	SET_VARSIZE(out, VARHDRSZ + len + hlen);

	ptr = VARDATA(out);

	memcpy(ptr, data, offsetof(struct_int64, elements));
	ptr += offsetof(struct_int64, elements);

	memcpy(ptr, data->elements, len);

	PG_RETURN_BYTEA_P(out);
}

Datum
trimmed_serial_numeric(PG_FUNCTION_ARGS)
{
	int				i;
	Size			hlen = offsetof(struct_numeric, elements);	/* header */
	Size			len;										/* elements */
	struct_numeric *data = (struct_numeric *)PG_GETARG_POINTER(0);
	bytea		   *out;
	char		   *ptr;

	CHECK_AGG_CONTEXT("trimmed_serial_numeric", fcinfo);

	/* sum sizes of all Numeric values to get the required size */
	len = 0;
	for (i = 0; i < data->next; i++)
		len += VARSIZE(data->elements[i]);

	out = (bytea *)palloc0(VARHDRSZ + len + hlen);
	SET_VARSIZE(out, VARHDRSZ + len + hlen);

	ptr = (char*) VARDATA(out);

	memcpy(ptr, data, offsetof(struct_numeric, elements));
	ptr += offsetof(struct_numeric, elements);

	/* now copy the contents of each Numeric value into the buffer */
	for (i = 0; i < data->next; i++)
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
	struct_double *out = (struct_double *)palloc(sizeof(struct_double));
	bytea  *data = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(data);
	char   *ptr = VARDATA(data);

	CHECK_AGG_CONTEXT("trimmed_deserial_double", fcinfo);

	Assert(len > 0);
	Assert(len % sizeof(double) == 0);

	/* copy the header */
	memcpy(out, ptr, offsetof(struct_double, elements));
	ptr += offsetof(struct_double, elements);

	Assert(len == offsetof(struct_double, elements) + out->next * sizeof(double));

	/* we only allocate the necessary space */
	out->elements = (double *)palloc(out->next * sizeof(double));
	out->nelements = out->next;

	memcpy((void *)out->elements, ptr, out->next * sizeof(double));

	PG_RETURN_POINTER(out);
}

Datum
trimmed_deserial_int32(PG_FUNCTION_ARGS)
{
	struct_int32 *out = (struct_int32 *)palloc(sizeof(struct_int32));
	bytea  *data = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(data);
	char   *ptr = VARDATA(data);

	CHECK_AGG_CONTEXT("trimmed_deserial_int32", fcinfo);

	Assert(len > 0);
	Assert(len % sizeof(int32) == 0);

	/* copy the header */
	memcpy(out, ptr, offsetof(struct_int32, elements));
	ptr += offsetof(struct_int32, elements);

	Assert(len == offsetof(struct_int32, elements) + out->next * sizeof(int32));

	/* we only allocate the necessary space */
	out->elements = (int32 *)palloc(out->next * sizeof(int32));
	out->nelements = out->next;

	memcpy((void *)out->elements, ptr, out->next * sizeof(int32));

	PG_RETURN_POINTER(out);
}

Datum
trimmed_deserial_int64(PG_FUNCTION_ARGS)
{
	struct_int64 *out = (struct_int64 *)palloc(sizeof(struct_int64));
	bytea  *data = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(data);
	char   *ptr = VARDATA(data);

	CHECK_AGG_CONTEXT("trimmed_deserial_int64", fcinfo);

	Assert(len > 0);
	Assert(len % sizeof(int64) == 0);

	/* copy the header */
	memcpy(out, ptr, offsetof(struct_int64, elements));
	ptr += offsetof(struct_int64, elements);

	Assert(len == offsetof(struct_int64, elements) + out->next * sizeof(int64));

	/* we only allocate the necessary space */
	out->elements = (int64 *)palloc(out->next * sizeof(int64));
	out->nelements = out->next;

	memcpy((void *)out->elements, ptr, out->next * sizeof(int64));

	PG_RETURN_POINTER(out);
}

Datum
trimmed_deserial_numeric(PG_FUNCTION_ARGS)
{
	int		i;
	struct_numeric *out = (struct_numeric *)palloc(sizeof(struct_numeric));
	bytea  *data = (bytea *)PG_GETARG_POINTER(0);
	Size	len = VARSIZE_ANY_EXHDR(data);
	char   *ptr = VARDATA(data);
	char   *tmp;

	CHECK_AGG_CONTEXT("trimmed_deserial_numeric", fcinfo);

	Assert(len > 0);

	/* first read the struct header, stored at the beginning */
	memcpy(out, ptr, offsetof(struct_numeric, elements));
	ptr += offsetof(struct_numeric, elements);

	/* allocate an array with enough space for the Numeric pointers */
	out->nelements = out->next; /* no slack space for new data */
	out->elements = (Numeric *)palloc(out->next * sizeof(Numeric));

	/*
	 * we also need to copy the Numeric contents, but instead of copying
	 * the values one by one, we copy the chunk of the serialized data
	 */
	tmp = palloc(len - sizeof(int));
	memcpy(tmp, ptr, len - sizeof(int));
	ptr = tmp;

	/* and now just set the pointers in the elements array */
	for (i = 0; i < out->next; i++)
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
	struct_double *data1;
	struct_double *data2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_double", fcinfo, agg_context);

	data1 = PG_ARGISNULL(0) ? NULL : (struct_double *) PG_GETARG_POINTER(0);
	data2 = PG_ARGISNULL(1) ? NULL : (struct_double *) PG_GETARG_POINTER(1);

	if (data2 == NULL)
		PG_RETURN_POINTER(data1);

	if (data1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		data1 = (struct_double *)palloc0(sizeof(struct_double));
		data1->nelements = data2->nelements;
		data1->next = data2->next;

		data1->cut_lower = data2->cut_lower;
		data1->cut_upper = data2->cut_upper;

		data1->elements = (double*)palloc0(sizeof(double) * data2->nelements);

		memcpy(data1->elements, data2->elements, sizeof(double) * data2->nelements);

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(data1);
	}

	Assert((data1 != NULL) && (data2 != NULL));

	/* if there's not enough space in data1, enlarge it */
	if (data1->next + data2->next >= data1->nelements)
	{
		/* we size the array to match the size exactly */
		data1->nelements = data1->next + data2->next;
		data1->elements = (double *)repalloc(data1->elements,
											 data1->nelements * sizeof(double));
	}

	/* copy the elements from data2 into data1 */
	memcpy(data1->elements + data1->next, data2->elements,
		   data2->next * sizeof(double));

	/* and finally remember the current number of elements */
	data1->next += data2->next;

	PG_RETURN_POINTER(data1);
}

Datum
trimmed_combine_int32(PG_FUNCTION_ARGS)
{
	struct_int32 *data1;
	struct_int32 *data2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_int32", fcinfo, agg_context);

	data1 = PG_ARGISNULL(0) ? NULL : (struct_int32 *) PG_GETARG_POINTER(0);
	data2 = PG_ARGISNULL(1) ? NULL : (struct_int32 *) PG_GETARG_POINTER(1);

	if (data2 == NULL)
		PG_RETURN_POINTER(data1);

	if (data1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		data1 = (struct_int32 *)palloc0(sizeof(struct_int32));
		data1->nelements = data2->nelements;
		data1->next = data2->next;

		data1->cut_lower = data2->cut_lower;
		data1->cut_upper = data2->cut_upper;

		data1->elements = (int32*)palloc0(sizeof(int32) * data2->nelements);

		memcpy(data1->elements, data2->elements, sizeof(int32) * data2->nelements);

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(data1);
	}

	Assert((data1 != NULL) && (data2 != NULL));

	/* if there's not enough space in data1, enlarge it */
	if (data1->next + data2->next >= data1->nelements)
	{
		/* we size the array to match the size exactly */
		data1->nelements = data1->next + data2->next;
		data1->elements = (int32 *)repalloc(data1->elements,
											 data1->nelements * sizeof(int32));
	}

	/* copy the elements from data2 into data1 */
	memcpy(data1->elements + data1->next, data2->elements,
		   data2->next * sizeof(int32));

	/* and finally remember the current number of elements */
	data1->next += data2->next;

	PG_RETURN_POINTER(data1);
}

Datum
trimmed_combine_int64(PG_FUNCTION_ARGS)
{
	struct_int64 *data1;
	struct_int64 *data2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_int64", fcinfo, agg_context);

	data1 = PG_ARGISNULL(0) ? NULL : (struct_int64 *) PG_GETARG_POINTER(0);
	data2 = PG_ARGISNULL(1) ? NULL : (struct_int64 *) PG_GETARG_POINTER(1);

	if (data2 == NULL)
		PG_RETURN_POINTER(data1);

	if (data1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		data1 = (struct_int64 *)palloc0(sizeof(struct_int64));
		data1->nelements = data2->nelements;
		data1->next = data2->next;

		data1->cut_lower = data2->cut_lower;
		data1->cut_upper = data2->cut_upper;

		data1->elements = (int64*)palloc0(sizeof(int64) * data2->nelements);

		memcpy(data1->elements, data2->elements, sizeof(int64) * data2->nelements);

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(data1);
	}

	/* if there's not enough space in data1, enlarge it */
	if (data1->next + data2->next >= data1->nelements)
	{
		/* we size the array to match the size exactly */
		data1->nelements = data1->next + data2->next;
		data1->elements = (int64 *)repalloc(data1->elements,
											 data1->nelements * sizeof(int64));
	}

	/* copy the elements from data2 into data1 */
	memcpy(data1->elements + data1->next, data2->elements,
		   data2->next * sizeof(int64));

	/* and finally remember the current number of elements */
	data1->next += data2->next;

	PG_RETURN_POINTER(data1);
}

Datum
trimmed_combine_numeric(PG_FUNCTION_ARGS)
{
	Size			len;
	int				i;
	struct_numeric *data1;
	struct_numeric *data2;
	MemoryContext agg_context;
	MemoryContext old_context;
	char		   *tmp;

	GET_AGG_CONTEXT("trimmed_combine_numeric", fcinfo, agg_context);

	data1 = PG_ARGISNULL(0) ? NULL : (struct_numeric *) PG_GETARG_POINTER(0);
	data2 = PG_ARGISNULL(1) ? NULL : (struct_numeric *) PG_GETARG_POINTER(1);

	if (data2 == NULL)
		PG_RETURN_POINTER(data1);

	if (data1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		data1 = (struct_numeric *)palloc0(sizeof(struct_numeric));
		data1->nelements = data2->nelements;
		data1->next = 0;

		data1->cut_lower = data2->cut_lower;
		data1->cut_upper = data2->cut_upper;

		data1->elements = (Numeric*)palloc0(sizeof(Numeric) * data2->nelements);

		len = 0;
		for (i = 0; i < data2->next; i++)
			len += VARSIZE(data2->elements[i]);

		tmp = palloc(len);

		for (i = 0; i < data2->next; i++)
		{
			memcpy(tmp, data2->elements[i], VARSIZE(data2->elements[i]));
			data1->elements[data1->next++] = (Numeric)tmp;
			tmp += VARSIZE(data2->elements[i]);
		}

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(data1);
	}

	/* if there's not enough space in data1, enlarge it */
	if (data1->next + data2->next >= data1->nelements)
	{
		/* we size the array to match the size exactly */
		data1->nelements = data1->next + data2->next;
		data1->elements = (Numeric *)repalloc(data1->elements,
											  data1->nelements * sizeof(Numeric));
	}

	/*
	 * we can't copy just the pointers - we need to copy the contents of the
	 * Numeric datums too - to save space, we'll copy them into a single buffer
	 * and use the pointers
	 */
	len = 0;
	for (i = 0; i < data2->next; i++)
		len += VARSIZE(data2->elements[i]);

	old_context = MemoryContextSwitchTo(agg_context);
	tmp = palloc(len);
	MemoryContextSwitchTo(old_context);

	for (i = 0; i < data2->next; i++)
	{
		memcpy(tmp, data2->elements[i], VARSIZE(data2->elements[i]));
		data1->elements[data1->next++] = (Numeric)tmp;
		tmp += VARSIZE(data2->elements[i]);
	}

	Assert(data1->next == data1->nelements);

	PG_RETURN_POINTER(data1);
}

Datum
trimmed_avg_double(PG_FUNCTION_ARGS)
{

    int     i;
    double  result = 0;
    int     from, to, cnt;

    struct_double * data;

    CHECK_AGG_CONTEXT("trimmed_avg_double", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_double*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(double), &double_comparator);

    for (i = from; i < to; i++) {
        result = result + data->elements[i]/cnt;
    }

    PG_RETURN_FLOAT8(result);

}

Datum
trimmed_double_array(PG_FUNCTION_ARGS)
{

    int     i;

    /* average, var_pop, var_samp, variance, stddev_pop, stddev_samp, stddev */
    double  result[7] = {0, 0, 0, 0, 0, 0, 0};

    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_double * data;

    CHECK_AGG_CONTEXT("trimmed_double_array", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_double*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(double), &double_comparator);

    /* average */
    result[0] = 0;
    result[1] = 1;
    result[2] = 2;
    for (i = from; i < to; i++) {
        result[0] += data->elements[i]/cnt;
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    result[1] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt);       /* var_pop */
    result[2] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)); /* var_samp */

    /* variance */
    result[3] = 0;
    for (i = from; i < to; i++) {
        result[3] += (data->elements[i] - result[0])*(data->elements[i] - result[0])/cnt;
    }

    result[4] = sqrt(result[1]); /* stddev_pop */
    result[5] = sqrt(result[2]); /* stddev_samp */
    result[6] = sqrt(result[3]); /* stddev */

    return double_to_array(fcinfo, result, 7);

}

Datum
trimmed_avg_int32(PG_FUNCTION_ARGS)
{

    int     i;
    double  result = 0;
    int     from, to, cnt;

    struct_int32 * data;

    CHECK_AGG_CONTEXT("trimmed_avg_int32", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int32*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int32), &int32_comparator);

    for (i = from; i < to; i++) {
        result = result + ((double)data->elements[i])/cnt;
    }

    PG_RETURN_FLOAT8(result);

}

Datum
trimmed_int32_array(PG_FUNCTION_ARGS)
{

    int     i;
    /* average, var_pop, var_samp, variance, stddev_pop, stddev_samp, stddev */
    double  result[7] = {0, 0, 0, 0, 0, 0, 0};

    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_int32 * data;

    CHECK_AGG_CONTEXT("trimmed_int32_array", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int32*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int32), &int32_comparator);

    /* average */
    result[0] = 0;
    result[1] = 0;
    result[2] = 0;
    for (i = from; i < to; i++) {
        result[0] += (double)data->elements[i]/cnt;
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + ((double)data->elements[i])*((double)data->elements[i]);
    }

    result[1] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt);       /* var_pop */
    result[2] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)); /* var_samp */

    /* variance */
    result[3] = 0;
    for (i = from; i < to; i++) {
        result[3] += ((double)data->elements[i] - result[0])*((double)data->elements[i] - result[0])/cnt;
    }

    result[4] = sqrt(result[1]); /* stddev_pop */
    result[5] = sqrt(result[2]); /* stddev_samp */
    result[6] = sqrt(result[3]); /* stddev */

    return double_to_array(fcinfo, result, 7);

}

Datum
trimmed_avg_int64(PG_FUNCTION_ARGS)
{

    int     i;
    double  result = 0;
    int     from, to, cnt;

    struct_int64 * data;

    CHECK_AGG_CONTEXT("trimmed_avg_int64", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int64*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int64), &int64_comparator);

    for (i = from; i < to; i++) {
        result = result + ((double)data->elements[i])/cnt;
    }

    PG_RETURN_FLOAT8(result);

}

Datum
trimmed_int64_array(PG_FUNCTION_ARGS)
{

    int     i;
    /* average, var_pop, var_samp, variance, stddev_pop, stddev_samp, stddev */
    double  result[7] = {0, 0, 0, 0, 0, 0, 0};

    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_int64 * data;

    CHECK_AGG_CONTEXT("trimmed_int64_array", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int64*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int64), &int64_comparator);

    /* average */
    result[0] = 0;
    result[1] = 0;
    result[2] = 0;
    for (i = from; i < to; i++) {
        result[0] += (double)data->elements[i]/cnt;
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + ((double)data->elements[i])*((double)data->elements[i]);
    }

    result[1] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt);       /* var_pop */
    result[2] = (cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)); /* var_samp */

    /* variance */
    result[3] = 0;
    for (i = from; i < to; i++) {
        result[3] += ((double)data->elements[i] - result[0])*((double)data->elements[i] - result[0])/cnt;
    }

    result[4] = sqrt(result[1]); /* stddev_pop */
    result[5] = sqrt(result[2]); /* stddev_samp */
    result[6] = sqrt(result[3]); /* stddev */

    return double_to_array(fcinfo, result, 7);

}

Datum
trimmed_avg_numeric(PG_FUNCTION_ARGS)
{

    int     i;
    Numeric result, cnt;
    int     from, to;

    struct_numeric * data;

    CHECK_AGG_CONTEXT("trimmed_avg_numeric", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_numeric*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);

    if (from > to) {
        PG_RETURN_NULL();
    }

    /* create numeric values */
    cnt    = create_numeric(to-from);
    result = create_numeric(0);

    qsort(data->elements, data->next, sizeof(Numeric), &numeric_comparator);

    for (i = from; i < to; i++) {
        result = add_numeric(result, div_numeric(data->elements[i], cnt));
    }

    PG_RETURN_NUMERIC(result);

}

Datum
trimmed_numeric_array(PG_FUNCTION_ARGS)
{

    int     i;
    /* average, var_pop, var_samp, variance, stddev_pop, stddev_samp, stddev */
    Numeric result[7];
    Numeric sum_x, sum_x2;

    Numeric cntNumeric, cntNumeric_1;
    int     from, to;

    struct_numeric * data;

    CHECK_AGG_CONTEXT("trimmed_numeric_array", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_numeric*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);

    if (from > to) {
        PG_RETURN_NULL();
    }

    /* create numeric values */
    cntNumeric = create_numeric(to-from);
    cntNumeric_1 = create_numeric(to-from-1);

    qsort(data->elements, data->next, sizeof(Numeric), &numeric_comparator);

    /* average */
    result[0] = create_numeric(0);
    result[1] = create_numeric(0);
    result[2] = create_numeric(0);

    sum_x   = create_numeric(0);
    sum_x2  = create_numeric(0);

    // cntNumeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int32GetDatum(cnt)));

    for (i = from; i < to; i++) {
        result[0]   = add_numeric(result[0], div_numeric(data->elements[i], cntNumeric));
        sum_x       = add_numeric(sum_x, data->elements[i]);
        sum_x2      = add_numeric(sum_x2, mul_numeric(data->elements[i], data->elements[i]));
    }

    result[1] = div_numeric(sub_numeric(mul_numeric(cntNumeric, sum_x2), mul_numeric(sum_x, sum_x)), mul_numeric(cntNumeric, cntNumeric)); /* var_pop */
    result[2] = div_numeric(sub_numeric(mul_numeric(cntNumeric, sum_x2), mul_numeric(sum_x, sum_x)), mul_numeric(cntNumeric, cntNumeric_1)); /* var_samp */

    /* variance */
    result[3] = create_numeric(0);
    for (i = from; i < to; i++) {
        result[3]   = add_numeric(result[3], div_numeric(mul_numeric(sub_numeric(data->elements[i],result[0]), sub_numeric(data->elements[i],result[0])), cntNumeric));
    }

    result[4] = sqrt_numeric(result[1]); /* stddev_pop */
    result[5] = sqrt_numeric(result[2]); /* stddev_samp */
    result[6] = sqrt_numeric(result[3]); /* stddev */

    return numeric_to_array(fcinfo, result, 7);

}

Datum
trimmed_var_double(PG_FUNCTION_ARGS)
{

    int     i;
    double  result = 0;
    double  avg = 0;
    int     from, to, cnt;

    struct_double * data;

    CHECK_AGG_CONTEXT("trimmed_var_double", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_double*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(double), &double_comparator);

    for (i = from; i < to; i++) {
        avg = avg + data->elements[i]/cnt;
    }

    for (i = from; i < to; i++) {
        result = result + (data->elements[i] - avg)*(data->elements[i] - avg)/cnt;
    }

    PG_RETURN_FLOAT8(result);

}

Datum
trimmed_var_int32(PG_FUNCTION_ARGS)
{

    int     i;
    double  result = 0;
    double  avg = 0;
    int     from, to, cnt;

    struct_int32 * data;

    CHECK_AGG_CONTEXT("trimmed_var_int32", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int32*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int32), &int32_comparator);

    for (i = from; i < to; i++) {
        avg = avg + ((double)data->elements[i])/cnt;
    }

    for (i = from; i < to; i++) {
        result = result + (data->elements[i] - avg)*(data->elements[i] - avg)/cnt;
    }

    PG_RETURN_FLOAT8(result);

}

Datum
trimmed_var_int64(PG_FUNCTION_ARGS)
{

    int     i;
    double  result = 0;
    double  avg = 0;
    int     from, to, cnt;

    struct_int64 * data;

    CHECK_AGG_CONTEXT("trimmed_var_int64", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int64*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int64), &int64_comparator);

    for (i = from; i < to; i++) {
        avg = avg + ((double)data->elements[i])/cnt;
    }

    for (i = from; i < to; i++) {
        result = result + (data->elements[i] - avg)*(data->elements[i] - avg)/cnt;
    }

    PG_RETURN_FLOAT8(result);

}

Datum
trimmed_var_numeric(PG_FUNCTION_ARGS)
{

    int     i;
    Numeric result, avg, cnt;
    int     from, to;

    struct_numeric * data;

    CHECK_AGG_CONTEXT("trimmed_var_numeric", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_numeric*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);

    if (from > to) {
        PG_RETURN_NULL();
    }

    cnt = create_numeric(to - from);
    avg = create_numeric(0);
    result = create_numeric(0);

    qsort(data->elements, data->next, sizeof(Numeric), &numeric_comparator);

    for (i = from; i < to; i++) {
        avg = add_numeric(avg, div_numeric(data->elements[i], cnt));
    }

    for (i = from; i < to; i++) {
        result = add_numeric(result, div_numeric(pow_numeric(sub_numeric(data->elements[i],avg),2),cnt));
    }

    PG_RETURN_NUMERIC(result);

}

Datum
trimmed_var_pop_double(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_double * data;

    CHECK_AGG_CONTEXT("trimmed_var_pop_double", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_double*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(double), &double_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt));

}

Datum
trimmed_var_pop_int32(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_int32 * data;

    CHECK_AGG_CONTEXT("trimmed_var_pop_int32", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int32*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int32), &int32_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt));

}

Datum
trimmed_var_pop_int64(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_int64 * data;

    CHECK_AGG_CONTEXT("trimmed_var_pop_int64", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int64*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int64), &int64_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt));

}

Datum
trimmed_var_pop_numeric(PG_FUNCTION_ARGS)
{

    int     i;
    Numeric sum_x, sum_x2, cnt;
    int     from, to;

    struct_numeric * data;

    CHECK_AGG_CONTEXT("trimmed_var_pop_numeric", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_numeric*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);

    if (from > to) {
        PG_RETURN_NULL();
    }

    cnt  = create_numeric(to - from);
    sum_x = create_numeric(0);
    sum_x2 = create_numeric(0);

    qsort(data->elements, data->next, sizeof(Numeric), &numeric_comparator);

    for (i = from; i < to; i++) {
        sum_x = add_numeric(sum_x, data->elements[i]);
        sum_x2 = add_numeric(sum_x2, mul_numeric(data->elements[i], data->elements[i]));
    }

    PG_RETURN_NUMERIC (div_numeric(sub_numeric(mul_numeric(cnt, sum_x2), mul_numeric(sum_x, sum_x)),
                                   mul_numeric(cnt, cnt)));

}

Datum
trimmed_var_samp_double(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_double * data;

    CHECK_AGG_CONTEXT("trimmed_var_samp_double", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_double*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(double), &double_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)));

}

Datum
trimmed_var_samp_int32(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_int32 * data;

    CHECK_AGG_CONTEXT("trimmed_var_samp_int32", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int32*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int32), &int32_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)));

}

Datum
trimmed_var_samp_int64(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_int64 * data;

    CHECK_AGG_CONTEXT("trimmed_var_samp_int64", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int64*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int64), &int64_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 ((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1)));

}

Datum
trimmed_var_samp_numeric(PG_FUNCTION_ARGS)
{

    int     i;
    Numeric sum_x, sum_x2, cnt;
    int     from, to;

    struct_numeric * data;

    CHECK_AGG_CONTEXT("trimmed_var_samp_numeric", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_numeric*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);

    if (from > to) {
        PG_RETURN_NULL();
    }

    cnt  = create_numeric(to - from);
    sum_x = create_numeric(0);
    sum_x2 = create_numeric(0);

    qsort(data->elements, data->next, sizeof(Numeric), &numeric_comparator);

    for (i = from; i < to; i++) {
        sum_x = add_numeric(sum_x, data->elements[i]);
        sum_x2 = add_numeric(sum_x2, mul_numeric(data->elements[i], data->elements[i]));
    }

    PG_RETURN_NUMERIC (div_numeric(sub_numeric(mul_numeric(cnt, sum_x2), mul_numeric(sum_x, sum_x)),
                                   mul_numeric(cnt, sub_numeric(cnt, create_numeric(1)))));

}

Datum
trimmed_stddev_double(PG_FUNCTION_ARGS)
{

    int     i;
    double  result = 0;
    double  avg = 0;
    int     from, to, cnt;

    struct_double * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_double", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_double*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(double), &double_comparator);

    for (i = from; i < to; i++) {
        avg = avg + data->elements[i]/cnt;
    }

    for (i = from; i < to; i++) {
        result = result + (data->elements[i] - avg)*(data->elements[i] - avg)/cnt;
    }

    PG_RETURN_FLOAT8 (sqrt(result));

}

Datum
trimmed_stddev_int32(PG_FUNCTION_ARGS)
{

    int     i;
    double  result = 0;
    double  avg = 0;
    int     from, to, cnt;

    struct_int32 * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_int32", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int32*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int32), &int32_comparator);

    for (i = from; i < to; i++) {
        avg = avg + ((double)data->elements[i])/cnt;
    }

    for (i = from; i < to; i++) {
        result = result + (data->elements[i] - avg)*(data->elements[i] - avg)/cnt;
    }

    PG_RETURN_FLOAT8 (sqrt(result));

}

Datum
trimmed_stddev_int64(PG_FUNCTION_ARGS)
{

    int     i;
    double  result = 0;
    double  avg = 0;
    int     from, to, cnt;

    struct_int64 * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_int64", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int64*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int64), &int64_comparator);

    for (i = from; i < to; i++) {
        avg = avg + ((double)data->elements[i])/cnt;
    }

    for (i = from; i < to; i++) {
        result = result + (data->elements[i] - avg)*(data->elements[i] - avg)/cnt;
    }

    PG_RETURN_FLOAT8 (sqrt(result));

}

Datum
trimmed_stddev_numeric(PG_FUNCTION_ARGS)
{

    int     i;
    Numeric result, avg, cnt;
    int     from, to;

    struct_numeric * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_numeric", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_numeric*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);

    if (from > to) {
        PG_RETURN_NULL();
    }

    cnt = create_numeric(to - from);
    avg = create_numeric(0);
    result = create_numeric(0);

    qsort(data->elements, data->next, sizeof(Numeric), &numeric_comparator);

    for (i = from; i < to; i++) {
        avg = add_numeric(avg, div_numeric(data->elements[i], cnt));
    }

    for (i = from; i < to; i++) {
        result = add_numeric(result, div_numeric(pow_numeric(sub_numeric(data->elements[i], avg), 2), cnt));
    }

    PG_RETURN_NUMERIC (sqrt_numeric(result));

}

Datum
trimmed_stddev_pop_double(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_double * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_pop_double", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_double*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(double), &double_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt)));

}

Datum
trimmed_stddev_pop_int32(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_int32 * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_pop_int32", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int32*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int32), &int32_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt)));

}

Datum
trimmed_stddev_pop_int64(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_int64 * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_pop_int64", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int64*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int64), &int64_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * cnt)));

}

Datum
trimmed_stddev_pop_numeric(PG_FUNCTION_ARGS)
{

    int     i;
    Numeric sum_x, sum_x2, cnt;
    int     from, to;

    struct_numeric * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_pop_numeric", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_numeric*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);

    if (from > to) {
        PG_RETURN_NULL();
    }

    cnt  = create_numeric(to - from);
    sum_x = create_numeric(0);
    sum_x2 = create_numeric(0);

    qsort(data->elements, data->next, sizeof(Numeric), &numeric_comparator);

    for (i = from; i < to; i++) {
        sum_x = add_numeric(sum_x, data->elements[i]);
        sum_x2 = add_numeric(sum_x2, mul_numeric(data->elements[i], data->elements[i]));
    }

    PG_RETURN_NUMERIC (sqrt_numeric(div_numeric(sub_numeric(mul_numeric(cnt, sum_x2),
                                                            mul_numeric(sum_x, sum_x)),
                                                pow_numeric(cnt, 2))));

}

Datum
trimmed_stddev_samp_double(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_double * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_samp_double", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_double*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(double), &double_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1))));

}

Datum
trimmed_stddev_samp_int32(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_int32 * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_samp_int32", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int32*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int32), &int32_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1))));

}

Datum
trimmed_stddev_samp_int64(PG_FUNCTION_ARGS)
{

    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;

    struct_int64 * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_samp_int64", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_int64*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);
    cnt  = (to - from);

    if (from > to) {
        PG_RETURN_NULL();
    }

    qsort(data->elements, data->next, sizeof(int64), &int64_comparator);

    for (i = from; i < to; i++) {
        sum_x = sum_x + data->elements[i];
        sum_x2 = sum_x2 + data->elements[i]*data->elements[i];
    }

    PG_RETURN_FLOAT8 (sqrt((cnt * sum_x2 - sum_x * sum_x) / (cnt * (cnt - 1))));

}

Datum
trimmed_stddev_samp_numeric(PG_FUNCTION_ARGS)
{

    int     i;
    Numeric sum_x, sum_x2, cnt;
    int     from, to;

    struct_numeric * data;

    CHECK_AGG_CONTEXT("trimmed_stddev_samp_numeric", fcinfo);

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    data = (struct_numeric*)PG_GETARG_POINTER(0);

    from = floor(data->next * data->cut_lower);
    to   = data->next - floor(data->next * data->cut_upper);

    if (from > to) {
        PG_RETURN_NULL();
    }

    cnt  = create_numeric(to - from);
    sum_x = create_numeric(0);
    sum_x2 = create_numeric(0);

    qsort(data->elements, data->next, sizeof(Numeric), &numeric_comparator);

    for (i = from; i < to; i++) {
        sum_x = add_numeric(sum_x, data->elements[i]);
        sum_x2 = add_numeric(sum_x2, pow_numeric(data->elements[i], 2));
    }

    PG_RETURN_NUMERIC (sqrt_numeric(div_numeric(sub_numeric(mul_numeric(cnt, sum_x2),
                                                            pow_numeric(sum_x, 2)),
                                                mul_numeric(cnt, sub_numeric(cnt, create_numeric(1))))));

}

static int  double_comparator(const void *a, const void *b) {
    double af = (*(double*)a);
    double bf = (*(double*)b);
    return (af > bf) - (af < bf);
}

static int  int32_comparator(const void *a, const void *b) {
    int32 af = (*(int32*)a);
    int32 bf = (*(int32*)b);
    return (af > bf) - (af < bf);
}

static int  int64_comparator(const void *a, const void *b) {
    int64 af = (*(int64*)a);
    int64 bf = (*(int64*)b);
    return (af > bf) - (af < bf);
}

static int  numeric_comparator(const void *a, const void *b) {

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

static Numeric create_numeric(int value) {

    FunctionCallInfoData fcinfo;

    /* set params */
    fcinfo.nargs = 1;
    fcinfo.arg[0] = Int32GetDatum(value);
    fcinfo.argnull[0] = false;

    /* return the result */
    return DatumGetNumeric(int4_numeric(&fcinfo));

}

static Numeric add_numeric(Numeric a, Numeric b) {

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

static Numeric div_numeric(Numeric a, Numeric b) {

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

static Numeric mul_numeric(Numeric a, Numeric b) {

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

static Numeric sub_numeric(Numeric a, Numeric b) {

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

static Numeric pow_numeric(Numeric a, int b) {

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

static Numeric sqrt_numeric(Numeric a) {

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
double_to_array(FunctionCallInfo fcinfo, double * d, int len) {

    ArrayBuildState *astate = NULL;
    int         i;

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
numeric_to_array(FunctionCallInfo fcinfo, Numeric * d, int len) {

    ArrayBuildState *astate = NULL;
    int         i;

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
