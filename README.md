Benchmark
=========

Benchmark is the macrobenchmarking framework for OpenSearch

What is Benchmark?
------------------

You want to benchmark OpenSearch? Then Benchmark is for you. It can help you with the following tasks:

* Setup and teardown of an OpenSearch cluster for benchmarking
* Management of benchmark data and specifications even across OpenSearch versions
* Running benchmarks and recording results
* Finding performance problems by attaching so-called telemetry devices
* Comparing performance results

We have also put considerable effort in Benchmark to ensure that benchmarking data are reproducible.

Quick Start
-----------

Benchmark is developed for Unix and is actively tested on Linux and MacOS. Benchmark supports [benchmarking OpenSearch clusters running on Windows](<https://github.com/opensearch-project/OpenSearch-Benchmark/blob/main/DEVELOPER_GUIDE.md>) but Benchmark itself needs to be installed on machines running Unix.

Installing Benchmark
--------------------------

**Note**: If you actively develop on OpenSearch, we recommend that you [install Benchmark in development mode](<https://github.com/opensearch-project/OpenSearch-Benchmark/blob/main/DEVELOPER_GUIDE.md>) instead as OpenSearch is fast moving and Benchmark always adapts accordingly to the latest main version.

Install Python 3.8+ including ``pip3``, git 1.9+ and an [appropriate JDK to run OpenSearch](<https://github.com/opensearch-project/OpenSearch-Benchmark/blob/main/DEVELOPER_GUIDE.md>) Be sure that ``JAVA_HOME`` points to that JDK. Then run the following command, optionally prefixed by ``sudo`` if necessary:

    pip3 install opensearch-benchmark


If you have any trouble or need more detailed instructions, please look in the [detailed installation guide](<https://github.com/opensearch-project/OpenSearch-Benchmark/blob/main/DEVELOPER_GUIDE.md>`).

Run your first test execution
-----------------------------

Now we're ready to run our first test execution:

    opensearch-benchmark execute_test --distribution-version=1.0.0 --workload=geonames --test-mode

This will download OpenSearch 1.0.0 and run Benchmark's default workload - the [geonames workload](<https://github.com/opensearch-project/opensearch-benchmark-workloads/tree/main/geonames>) - against it. 
Note that this uses the `--test-mode` argument to only run a single instance of each operation in order to reduce the time needed for a test execution. This argument is used as a sanity check and should be removed in an actual benchmarking scenario.
After the test execution, a summary report is written to the command line:

    ------------------------------------------------------
        _______             __   _____
       / ____(_)___  ____ _/ /  / ___/_________  ________
      / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \
     / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/
    /_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/
    ------------------------------------------------------

    |                         Metric |                 Task |     Value |   Unit |
    |-------------------------------:|---------------------:|----------:|-------:|
    |            Total indexing time |                      |   28.0997 |    min |
    |               Total merge time |                      |   6.84378 |    min |
    |             Total refresh time |                      |   3.06045 |    min |
    |               Total flush time |                      |  0.106517 |    min |
    |      Total merge throttle time |                      |   1.28193 |    min |
    |               Median CPU usage |                      |     471.6 |      % |
    |             Total Young Gen GC |                      |    16.237 |      s |
    |               Total Old Gen GC |                      |     1.796 |      s |
    |                     Index size |                      |   2.60124 |     GB |
    |                  Total written |                      |   11.8144 |     GB |
    |         Heap used for segments |                      |   14.7326 |     MB |
    |       Heap used for doc values |                      |  0.115917 |     MB |
    |            Heap used for terms |                      |   13.3203 |     MB |
    |            Heap used for norms |                      | 0.0734253 |     MB |
    |           Heap used for points |                      |    0.5793 |     MB |
    |    Heap used for stored fields |                      |  0.643608 |     MB |
    |                  Segment count |                      |        97 |        |
    |                 Min Throughput |         index-append |   31925.2 | docs/s |
    |              Median Throughput |         index-append |   39137.5 | docs/s |
    |                 Max Throughput |         index-append |   39633.6 | docs/s |
    |      50.0th percentile latency |         index-append |   872.513 |     ms |
    |      90.0th percentile latency |         index-append |   1457.13 |     ms |
    |      99.0th percentile latency |         index-append |   1874.89 |     ms |
    |       100th percentile latency |         index-append |   2711.71 |     ms |
    | 50.0th percentile service time |         index-append |   872.513 |     ms |
    | 90.0th percentile service time |         index-append |   1457.13 |     ms |
    | 99.0th percentile service time |         index-append |   1874.89 |     ms |
    |  100th percentile service time |         index-append |   2711.71 |     ms |
    |                           ...  |                  ... |       ... |    ... |
    |                           ...  |                  ... |       ... |    ... |
    |                 Min Throughput |     painless_dynamic |   2.53292 |  ops/s |
    |              Median Throughput |     painless_dynamic |   2.53813 |  ops/s |
    |                 Max Throughput |     painless_dynamic |   2.54401 |  ops/s |
    |      50.0th percentile latency |     painless_dynamic |    172208 |     ms |
    |      90.0th percentile latency |     painless_dynamic |    310401 |     ms |
    |      99.0th percentile latency |     painless_dynamic |    341341 |     ms |
    |      99.9th percentile latency |     painless_dynamic |    344404 |     ms |
    |       100th percentile latency |     painless_dynamic |    344754 |     ms |
    | 50.0th percentile service time |     painless_dynamic |    393.02 |     ms |
    | 90.0th percentile service time |     painless_dynamic |   407.579 |     ms |
    | 99.0th percentile service time |     painless_dynamic |   430.806 |     ms |
    | 99.9th percentile service time |     painless_dynamic |   457.352 |     ms |
    |  100th percentile service time |     painless_dynamic |   459.474 |     ms |

    ----------------------------------
    [INFO] SUCCESS (took 2634 seconds)
    ----------------------------------


Getting help
------------

* Quick help: ``opensearch-benchmark --help``
* Look in [Benchmark's user guide](<https://github.com/opensearch-project/OpenSearch-Benchmark/blob/main/DEVELOPER_GUIDE.md>) for more information
* For any questions or answers, visit our [community forum](<https://discuss.opendistrocommunity.dev/>).
* File improvements or bug reports in our [Github repo](<https://github.com/opensearch-project/OpenSearch-Benchmark/issues>).

How to Contribute
-----------------

See all details in the [contributor guidelines](<https://github.com/opensearch-project/OpenSearch-Benchmark/blob/main/CONTRIBUTING.md>).

License
-------

This software is licensed under the Apache License, version 2 ("ALv2"), quoted below.

Copyright 2015-2021 OpenSearch <https://opensearch.org/>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
