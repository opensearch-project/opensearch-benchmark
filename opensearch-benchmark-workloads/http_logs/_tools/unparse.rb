require "json"
require "time"

################
#
# Reconstructs (un-parses) the existing http_logs corpora (data set). The introduction of ingest node pipelines
# requires the data to be JSON, but un-parsed log lines. This script was used to create the `http_logs_unparsed`, which
# is a mirror copy of "http_logs`, except it is un-parsed AND the timestamp is ISO8601 (not epoch_seconds)
#
# The output of this is is a file with lines of JSON that appear as follows:
#
# {"message" : "30.87.8.0 - - [1998-05-24T15:00:01-05:00] \"GET /images/info.gif HTTP/1.0\" 200 1251"}
# {"message" : "28.87.8.0 - - [1998-05-24T15:00:01-05:00] \"GET /french/images/hm_official.gif HTTP/1.1\" 200 972"}
# {"message" : "17.87.8.0 - - [1998-05-24T15:00:01-05:00] \"GET /french/hosts/cfo/images/cfo/cfophot3.jpg HTTP/1.0\" 200 6695"}
#
# Usage:
#
# rm *.unparse.json
# rm *.bz2
#
# wget http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/http_logs/documents-181998.json.bz2
# bunzip2 documents-181998.json.bz2
#
# wget http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/http_logs/documents-191998.json.bz2
# bunzip2 documents-191998.json.bz2
#
# wget http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/http_logs/documents-201998.json.bz2
# bunzip2 documents-201998.json.bz2
#
# wget http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/http_logs/documents-211998.json.bz2
# bunzip2 documents-211998.json.bz2
#
# wget http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/http_logs/documents-221998.json.bz2
# bunzip2 documents-221998.json.bz2
#
# wget http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/http_logs/documents-231998.json.bz2
# bunzip2 documents-231998.json.bz2
#
# wget http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/http_logs/documents-241998.json.bz2
# bunzip2 documents-241998.json.bz2
#
# ruby unparse.rb .
#
# #############

def self.getValue(data,key)
  data[key].nil? ? "-" : data[key].to_s
end

threads = 4
running = 0
Dir.glob(File.join(ARGV[0], "*.json")).select do |file|
  File.open(file.gsub('json', 'unparsed.json'), 'w') do |json_file|
    while running >= threads
      sleep 1
    end
    running = running + 1
    Thread.new do
      i = 0;
      File.open(file).each do |line|
        begin
          i += 1;
          print "." if i % 10000 == 0
          data = JSON.parse(line)
          logline = getValue(data,'clientip')  + " - - [" + Time.at(data['@timestamp'].to_i).iso8601 + "] \\\"" + getValue(data,'request') + "\\\" " + getValue(data,'status') + " " + getValue(data,'size')
          json_log_line = "{\"message\" : \"" + logline + "\"}\n"
          #TODO: validate this is proper JSON. ~15 rows (.02%) were post modified to remove an invalid '\' char in the resultant JSON
          json_file.write(json_log_line)
        rescue => e
          puts e
        end
      end
      running = running - 1
    end
    while running > 0
      sleep 1
    end
  end
end