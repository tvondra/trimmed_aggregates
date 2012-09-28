Trimmed aggregates
==================
This PostgreSQL extension provides several aggregate functions that
trim the input data set before applying the function, i.e. remove
lowest/highest values. The number of values to be removed is configured
using the parameters.

WARNING: Those aggregates require the whole set, as they need to collect
and sort the whole data set ((to trim low/high values). This may be
a time consuming process and require a lot of memory. Keep this in mind
when using those functions.


Available aggregates
--------------------
The extension implements aggregates that resemble those described here:
http://www.postgresql.org/docs/9.1/static/functions-aggregate.html, i.e.
AVG, VARIANCE, VAR_POP, VAR_SAMP, STDDEV, STDDEV_POP and STDDEV_SAMP

* AVG

        avg_trimmed(value, low_cut, high_cut)

* VARIANCE

        var_trimmed(value, low_cut, high_cut);
        var_pop_trimmed(value, low_cut, high_cut)
        var_samp_trimmed(value, low_cut, high_cut)

* STDDEV (standard deviation)

        stddev_trimmed(value, low_cut, high_cut)
        stddev_pop_trimmed(value, low_cut, high_cut)
        stddev_samp_trimmed(value, low_cut, high_cut)

* combined aggregate (computes all seven values at once)

        trimmed(value, low_cut, high_cut)

All those functions are overloaded for numeric, double precision, int32
and int64 data types.

Using the aggregates
--------------------
All the aggregates are used the same way so let's see how to use the
avg_trimmed aggregate. For example this

    SELECT avg_trimmed(i, 0.1, 0.1) FROM generate_series(1,1000) s(i);

means 10% of the values will be removed on both ends, and the average
will be computed using the middle 80%. On the other hand this

    SELECT avg_trimmed(i, 0.2, 0.1) FROM generate_series(1,1000) s(i);

means 20% of the lowest and 10% of the highest values will be removed,
so the average will be computed using the remaining 70% of values.

The combined aggregate computes and returns all values at once as an
array. The values are stored in this order

* average
* var_pop
* var_samp
* variance
* stddev_pop
* stddev_samp
* stddev

If you need more of the values at once this may be much more efficient
as it shares the memory and can compute the values with only two passes
through the data (to compute exact variance and stddev).

Installation
------------
Installing this extension is very simple - if you're using pgxn client
(and you should), just do this:

    $ pgxn install --testing trimmed_aggregates
    $ pgxn load --testing -d mydb trimmed_aggregates

You can also install manually, just it like any other extension, i.e.

    $ make install
    $ psql dbname -c "CREATE EXTENSION trimmed_averages"

And if you're on an older PostgreSQL version, you have to run the SQL
script manually (use the proper version).

    $ psql dbname < trimmed_averages--1.0.sql

That's all.


License
-------
This software is distributed under the terms of BSD 2-clause license.
See LICENSE or http://www.opensource.org/licenses/bsd-license.php for
more details.
