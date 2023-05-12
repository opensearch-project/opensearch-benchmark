#!/usr/bin/env python3
#
# expand-data-corpus.py - Generate a new data corpus from the http_logs corpus
#
# Takes one of the http_logs corpora files as input and duplicates it
# as specified; then emits the documents with a modified timestamp
# sequence.  Also generates the associated offset file to enable OSB
# to start up faster.
#
# See the help message for more information.
#

import os
import sys
import signal
import argparse
import json
import configparser

help_msg = """

NOTE: This is a beta feature.  The user model, interface and options
are subject to change.

This tool is intended for the purpose of expanding the size of the
data corpus associated an OSB workload.  Currently, this capability is
implemented only for the http_logs workload.

TLDR: to generate a 100 GB corpus and then run a test against it:

$ expand-data-corpus.py --corpus-size 100 --output-file-suffix 100gb

$ opensearch-benchmark execute-test --workload http_logs \\
    --workload_params=generated_corpus:t ...

The script generates new documents by duplicating ones in the existing
corpus files, while modifying the timestamp field.  It takes several
arguments, listed below.  The two primary ones deal with specifying
the desired target corpus size and the corpus name.  The remaining
options, tagged with "EXPERT", are mainly intended for advanced users.

Duplication of documents will lead to the fields other than the
timestamp potentially recurring multiple times.  However, the efficacy
of the queries should not be impacted if the script parameters are
selected appropriately as indicated below.

The larger data corpus would be suitable for running the workload at a
larger scale on, for instance, clusters with multiple data nodes or
featuring an upgraded instance type.

To carry out an performance test using the generated corpus, the
following flag needs to be passed to the OSB command:

    --workload-params=generated_corpus:t

Additional workload parameters can be added to the "workload-params"
option as desired.

The script can be invoked multiple times to create several corpora
that will all be loaded concurrently.

Prerequisites:

  * The data corpus files associated with http_logs need to already
    have been downloaded.  The easiest way to do this is to carry out
    a normal run, perhaps by limiting it to indexing-only.

  * The input file should be one of the data corpus files downloaded
    from the http_logs OSB workloads repository.  The script cues off
    the text alignment in those files.

Notes and limitations:

  * This feature is currently available only for OpenSearch clusters
    and Elasticsearch 7 cluster.

  * The options tagged with "EXPERT" are intended for advanced users
    and should not be needed in normal use.

  * There is currently no mechanism provided to manage the generated
    corpora files.  The workaround for now is to delete them manually
    from the http_logs data directory, and the associated gen-* files
    in the http_logs workload directory.  New ones can be regenerated
    subsequently, if desired.

  * OSB runs with and without the 'generated_corpus' flag should not
    generally be interleaved, since they target different
    indices. However, OSB can be run in ingest-only mode to ingest
    both the generated and default corpora in two separate runs.  Once
    ingested, queries packaged with the workload will operate on the
    entire loaded data set.

  * To be compliant with the time ranges used in the existing queries,
    the interval parameter is set to -2 for every 1 GB corpus size.
    This generates ~20 documents per timestamp for a 10 GB size.

  * The script is not currently optimized for speed.  Allow for about
    ~30 min for every 100 GB.

Usage:

    Use the -h option to view script usage and options.

"""

def handler(signum, frame):
    sys.exit(1)

class DocGenerator:

    def __init__(self,
                 input_file: str,
                 start_timestamp: int,
                 interval:int) -> None:
        self.input_file = input_file
        self.timestamp = start_timestamp
        self.interval = interval

    def get_next_doc(self):
        line_num = 0
        ndocs_per_ts = -self.interval if self.interval < 0 else 1

        with open(self.input_file) as fh:
            while True:
                fh.seek(0)
                for line in fh:
                    s = '{"@timestamp": ' + str(self.timestamp) + line[24:]

                    # Increment timestamp.
                    if self.interval < 0:
                        if line_num % ndocs_per_ts == 0:
                            self.timestamp += 1
                    else:
                        self.timestamp += self.interval

                    yield s
                    line_num += 1


class ArgParser(argparse.ArgumentParser):
    def usage_msg(self, message: str=None) -> None:
        if message:
            print(message, file=sys.stderr)
        print(file=sys.stderr)
        self.print_help(sys.stderr)
        sys.exit(1)

    def error(self, message):
        print('error: %s' % message, file=sys.stderr)
        self.usage_msg()


def generate_docs(workload: str,
                  repository: str,
                  input_file: str,
                  output_file_suffix: str,
                  n_docs: int,
                  corpus_size: int,
                  interval: int,
                  start_timestamp: int,
                  batch_size: int):

    #
    # Set up for generation.
    #
    config = configparser.ConfigParser()
    benchmark_home = os.environ.get('BENCHMARK_HOME') or os.environ['HOME']
    config.read(benchmark_home + '/.benchmark/benchmark.ini')

    root_dir = config['node']['root.dir']
    workload_dir= root_dir + '/workloads/' + repository + '/' + workload
    data_dir = config['benchmarks']['local.dataset.cache'] + '/' + workload

    output_file = data_dir + '/documents-' + output_file_suffix + '.json'
    if '/' not in input_file:
        input_file = data_dir + '/' + input_file

    out = open(output_file, 'w')
    offsets = open(output_file + '.offset', 'w')

    #
    # Obtain the generator to synthesize the documents.
    #
    g = DocGenerator(input_file, start_timestamp, interval).\
                        get_next_doc()

    #
    # Generate the desired number of documents.
    #
    line_num = 0
    offset = 0

    while True:
        if n_docs and line_num >= n_docs:
            break

        if corpus_size and offset >= corpus_size * 1000**3:
            break

        # Offset file entry.
        if line_num > 0 and line_num % batch_size == 0:
            s = str(line_num) + ';' + str(offset) + '\n'
            offsets.write(s)

        line = next(g)
        out.write(line)
        offset += len(line)
        line_num += 1

    out.close()
    offsets.close()

    #
    # Create the metadata files.
    #
    corpus_spec = dict()
    corpus_spec['target-index'] = 'logs-' + output_file_suffix
    corpus_spec['source-file'] = output_file
    corpus_spec['document-count'] = line_num
    corpus_spec['uncompressed-bytes'] = offset

    out = open(workload_dir + '/gen-docs-' + output_file_suffix + '.json', 'w')
    out.write(json.dumps(corpus_spec) + '\n')
    out.close()

    idx_spec = dict()
    idx_spec['name'] = 'logs-' + output_file_suffix
    idx_spec['body'] = 'index.json'

    out = open(workload_dir + '/gen-idx-' + output_file_suffix + '.json', 'w')
    out.write(json.dumps(idx_spec) + '\n')
    out.close()


def main(args: list) -> None:

    signal.signal(signal.SIGINT, handler)
    script_name = os.path.basename(__file__)

    parser = ArgParser(description=help_msg,
		formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-w', '--workload',
                        default='http_logs',
                        help="workload name, default: %(default)s")
    parser.add_argument('-r', '--workload-repository', default='default',
                        help="workload name, default: %(default)s")
    parser.add_argument('-c', '--corpus-size', type=int,
                        help="size of corpus to generate in GB")
    parser.add_argument('-o', '--output-file-suffix',
                        default='generated',
                        help="suffix for output file name, "
                        "documents-SUFFIX.json, default: %(default)s")
    parser.add_argument('-f', '--input-file',
                        default='documents-241998.json',
                        help="[EXPERT] input file name, default: %(default)s")
    parser.add_argument('-n', '--number-of-docs', type=int,
                        help="[EXPERT] number of documents to generate")
    parser.add_argument('-i', '--interval', type=int,
                        help="[EXPERT] interval between consecutive "
	        "timestamps, use a negative number to specify multiple "
		"docs per timestamp")
    parser.add_argument('-t', '--start-timestamp', type=int,
                        default=893964618,
                        help="[EXPERT] start timestamp, default: %(default)d")
    parser.add_argument('-b', '--batch-size', default=50000,
                        help="[EXPERT] batch size per OSB client thread, "
                        "default: %(default)d")

    args = parser.parse_args()

    workload = args.workload
    repository = args.workload_repository
    input_file = args.input_file
    output_file_suffix = args.output_file_suffix
    n_docs = args.number_of_docs
    corpus_size = args.corpus_size
    interval = args.interval if args.interval is not None else \
			corpus_size * -2
    start_timestamp = args.start_timestamp
    batch_size = args.batch_size

    if n_docs and corpus_size:
        parser.usage_msg(script_name +
                     ": can specify either number of documents"
                     "or corpus size, but not both")
    elif not n_docs and not corpus_size:
        parser.usage_msg(script_name +
                     ": must specify number of documents or corpus size")

    if workload != 'http_logs':
        parser.usage_msg(script_name +
                     ': only the "http_logs" workload is currently supported')

    generate_docs(workload,
                  repository,
                  input_file,
                  output_file_suffix,
                  n_docs,
                  corpus_size,
                  interval,
                  start_timestamp,
                  batch_size)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
