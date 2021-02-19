# Ignite performance statistics example
This example demonstrates how to collect performance statistics and build the report.
1. Run several nodes. Use the `ExampleNodeStartup` to run empty servers.
2. Run the example by the `PerformanceStatisticsExample`.
It starts statistics collection and simulates cluster workload. The default duration is 60 seconds.

   Performance statistics will be collected at Ignite work directory: `Ignite_work_directory/perf_stat/`.

3. Run the script from release package of the `ignite-performance-statistics-ext` extension:
    `./performance-statistics-tool/build-report.sh path_to_perf_stat_dir`

The report will be created in the new directory under the performance statistics files path:

    path_to_perf_stat_dir/report_yyyy-MM-dd_HH-mm-ss/

Open `report_yyyy-MM-dd_HH-mm-ss/index.html` in the browser to see the report.
