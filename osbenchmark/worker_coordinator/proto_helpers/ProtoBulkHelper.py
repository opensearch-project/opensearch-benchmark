from opensearch_protos.protos.schemas import document_pb2

def _parse_docs_from_body(body):
    lineSplitBody = body.decode('utf-8').split('\n')
    lineList = []
    opList = []
    docList = []
    indexPattern = '{"index":'
    for lineBody in lineSplitBody:
        lineList.append(lineBody)
        if indexPattern in lineBody:
            opList.append(lineBody)
        else:
            docList.append(lineBody)\
    # Remove empty line at end of body
    return docList[:-1]

class ProtoBulkHelper:
    @staticmethod
    def build_proto_request(params):
        index = params.get("index")
        body = params.get("body")

        name = params.get("name") # index-append
        op_type = params.get("operation-type") # op_type: bulk
        # ingest_perc = params.get("ingest-percentage")
        # bulk_size = params.get("bulk-size")

        # UN-USED BY CURRENT OP TYPES
        # include_pub = params.get("include-in-results_publishing")
        # type = params.get("type")
        # action_metadata_present = params.get("action-metadata-present")

        doc_list = _parse_docs_from_body(body)
        request  = document_pb2.BulkRequest()
        request.index = index
        for doc in doc_list:
            requestBody = document_pb2.BulkRequestBody()
            requestBody.doc = doc.encode('utf-8')
            index_op = document_pb2.IndexOperation()
            requestBody.index.CopyFrom(index_op)
            request.request_body.append(requestBody)
        return request

    @staticmethod
    def build_simple_stats(response, params):
        respSuccess = None
        which_field = response.WhichOneof('response')
        if which_field == 'bulk_response_body':
            respSuccess = response.bulk_response_body
        elif which_field == 'bulk_error_response':
            print(response.bulk_error_response)
            exit()  # REMOVE THIS AFTER TESTING - REQ FAILURE SET IN STATS BELOW
        else:
            exit()  # Fatal error

        stats = {
            "took": respSuccess.took,
            "success": not respSuccess.errors,  # true if an op failed
            "success-count": params.get("bulk-size"),
            "error-count": 0 # REMOVE THIS AFTER TESTING - SEE ABOVE
        }

        meta_data = {
            "index": params.get("index"),
            "weight": params.get("bulk-size"),
            "unit": params.get("unit"),
        }

        meta_data.update(stats)
        if not stats["success"]:
            meta_data["error-type"] = "bulk"
        return meta_data