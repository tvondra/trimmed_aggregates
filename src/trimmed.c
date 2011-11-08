#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "postgres.h"
#include "utils/palloc.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "nodes/memnodes.h"
#include "fmgr.h"
#include "catalog/pg_type.h"

#include "funcapi.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#if __WORDSIZE == 64
#define GET_POINTER(a,b)    (a*)(b)
#define GET_INTEGER(a)      ((int64)a)
#else
#define GET_POINTER(a,b)    (a*)((int32)b)
#define GET_INTEGER(a)      ((int32)a)
#endif

#define GET_AGG_CONTEXT(fname, fcinfo, aggcontext)  \
    if (! AggCheckCallContext(fcinfo, &aggcontext)) {   \
        elog(ERROR, "%s called in non-aggregate context", fname);  \
    }

#define SLICE_SIZE 1024

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

/* comparators, used for qsort */

static int  double_comparator(const void *a, const void *b);
static int  int32_comparator(const void *a, const void *b);
static int  int64_comparator(const void *a, const void *b);

/* ACCUMULATE DATA */

PG_FUNCTION_INFO_V1(trimmed_append_double);
PG_FUNCTION_INFO_V1(trimmed_append_int32);
PG_FUNCTION_INFO_V1(trimmed_append_int64);

Datum trimmed_append_double(PG_FUNCTION_ARGS);
Datum trimmed_append_int32(PG_FUNCTION_ARGS);
Datum trimmed_append_int64(PG_FUNCTION_ARGS);

/* AVERAGE */

PG_FUNCTION_INFO_V1(trimmed_avg_double);
PG_FUNCTION_INFO_V1(trimmed_avg_int32);
PG_FUNCTION_INFO_V1(trimmed_avg_int64);

Datum trimmed_avg_double(PG_FUNCTION_ARGS);
Datum trimmed_avg_int32(PG_FUNCTION_ARGS);
Datum trimmed_avg_int64(PG_FUNCTION_ARGS);

/* VARIANCE */

/* exact */
PG_FUNCTION_INFO_V1(trimmed_var_double);
PG_FUNCTION_INFO_V1(trimmed_var_int32);
PG_FUNCTION_INFO_V1(trimmed_var_int64);

Datum trimmed_var_double(PG_FUNCTION_ARGS);
Datum trimmed_var_int32(PG_FUNCTION_ARGS);
Datum trimmed_var_int64(PG_FUNCTION_ARGS);

/* population estimate */
PG_FUNCTION_INFO_V1(trimmed_var_pop_double);
PG_FUNCTION_INFO_V1(trimmed_var_pop_int32);
PG_FUNCTION_INFO_V1(trimmed_var_pop_int64);

Datum trimmed_var_pop_double(PG_FUNCTION_ARGS);
Datum trimmed_var_pop_int32(PG_FUNCTION_ARGS);
Datum trimmed_var_pop_int64(PG_FUNCTION_ARGS);

/* sample estimate */
PG_FUNCTION_INFO_V1(trimmed_var_samp_double);
PG_FUNCTION_INFO_V1(trimmed_var_samp_int32);
PG_FUNCTION_INFO_V1(trimmed_var_samp_int64);

Datum trimmed_var_samp_double(PG_FUNCTION_ARGS);
Datum trimmed_var_samp_int32(PG_FUNCTION_ARGS);
Datum trimmed_var_samp_int64(PG_FUNCTION_ARGS);

/* STANDARD DEVIATION */

/* exact */
PG_FUNCTION_INFO_V1(trimmed_stddev_double);
PG_FUNCTION_INFO_V1(trimmed_stddev_int32);
PG_FUNCTION_INFO_V1(trimmed_stddev_int64);

Datum trimmed_stddev_double(PG_FUNCTION_ARGS);
Datum trimmed_stddev_int32(PG_FUNCTION_ARGS);
Datum trimmed_stddev_int64(PG_FUNCTION_ARGS);

/* population estimate */
PG_FUNCTION_INFO_V1(trimmed_stddev_pop_double);
PG_FUNCTION_INFO_V1(trimmed_stddev_pop_int32);
PG_FUNCTION_INFO_V1(trimmed_stddev_pop_int64);

Datum trimmed_stddev_pop_double(PG_FUNCTION_ARGS);
Datum trimmed_stddev_pop_int32(PG_FUNCTION_ARGS);
Datum trimmed_stddev_pop_int64(PG_FUNCTION_ARGS);

/* sample estimate */
PG_FUNCTION_INFO_V1(trimmed_stddev_samp_double);
PG_FUNCTION_INFO_V1(trimmed_stddev_samp_int32);
PG_FUNCTION_INFO_V1(trimmed_stddev_samp_int64);

Datum trimmed_stddev_samp_double(PG_FUNCTION_ARGS);
Datum trimmed_stddev_samp_int32(PG_FUNCTION_ARGS);
Datum trimmed_stddev_samp_int64(PG_FUNCTION_ARGS);


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
        data = GET_POINTER(struct_double, PG_GETARG_INT64(0));
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
    
    PG_RETURN_INT64(GET_INTEGER(data));

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
        data = GET_POINTER(struct_int32, PG_GETARG_INT64(0));
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
    
    PG_RETURN_INT64(GET_INTEGER(data));

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
        data = GET_POINTER(struct_int64, PG_GETARG_INT64(0));
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
    
    PG_RETURN_INT64(GET_INTEGER(data));

}

Datum
trimmed_avg_double(PG_FUNCTION_ARGS)
{
    
    int     i;
    double  result = 0;
    int     from, to, cnt;
    
    struct_double * data;
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_avg_double", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_double, PG_GETARG_INT64(0));
    
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
trimmed_avg_int32(PG_FUNCTION_ARGS)
{
    
    int     i;
    double  result = 0;
    int     from, to, cnt;
    
    struct_int32 * data;
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_avg_int32", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int32, PG_GETARG_INT64(0));
    
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
trimmed_avg_int64(PG_FUNCTION_ARGS)
{
    
    int     i;
    double  result = 0;
    int     from, to, cnt;
    
    struct_int64 * data;
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_avg_int64", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int64, PG_GETARG_INT64(0));
    
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
trimmed_var_double(PG_FUNCTION_ARGS)
{
    
    int     i;
    double  result = 0;
    double  avg = 0;
    int     from, to, cnt;
    
    struct_double * data;
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_var_double", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_double, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_var_int32", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int32, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_var_int64", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int64, PG_GETARG_INT64(0));
    
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
trimmed_var_pop_double(PG_FUNCTION_ARGS)
{
    
    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;
    
    
    struct_double * data;
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_var_pop_double", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_double, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_var_pop_int32", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int32, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_var_pop_int64", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int64, PG_GETARG_INT64(0));
    
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
trimmed_var_samp_double(PG_FUNCTION_ARGS)
{
    
    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;
    
    
    struct_double * data;
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_var_samp_double", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_double, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_var_samp_int32", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int32, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_var_samp_int64", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int64, PG_GETARG_INT64(0));
    
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
trimmed_stddev_double(PG_FUNCTION_ARGS)
{
    
    int     i;
    double  result = 0;
    double  avg = 0;
    int     from, to, cnt;
    
    struct_double * data;
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_stddev_double", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_double, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_stddev_int32", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int32, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_stddev_int64", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int64, PG_GETARG_INT64(0));
    
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
trimmed_stddev_pop_double(PG_FUNCTION_ARGS)
{
    
    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;
    
    
    struct_double * data;
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_stddev_pop_double", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_double, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_stddev_pop_int32", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int32, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_stddev_pop_int64", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int64, PG_GETARG_INT64(0));
    
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
trimmed_stddev_samp_double(PG_FUNCTION_ARGS)
{
    
    int     i;
    double  sum_x = 0, sum_x2 = 0;
    int     from, to, cnt;
    
    
    struct_double * data;
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_stddev_samp_double", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_double, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_stddev_samp_int32", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int32, PG_GETARG_INT64(0));
    
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
    
    MemoryContext aggcontext;
    
    GET_AGG_CONTEXT("trimmed_stddev_samp_int64", fcinfo, aggcontext);
    
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    
    data = GET_POINTER(struct_int64, PG_GETARG_INT64(0));
    
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
