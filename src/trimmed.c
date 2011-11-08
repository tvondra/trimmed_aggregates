#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "postgres.h"
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
        
        data->elements[data->next++] = element;
        
    }
    
    MemoryContextSwitchTo(oldcontext);
    
    PG_RETURN_POINTER(data);

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