from opensearch.protobufs.schemas import document_pb2

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
    """
    Helper methods to bulk ingest workload dataset with protobuf.
    """

    """
    Build protobuf SearchRequest.
    Consumed from params dictionary:
    * ``body``: JSON body of bulk ingest request
    * ``index``: index name
    """
    @staticmethod
    def build_proto_request(params):
        index = params.get("index")
        body = params.get("body")
        doc_list = _parse_docs_from_body(body)
        request  = document_pb2.BulkRequest()
        request.index = index
        # All bulk request here are index ops
        op_cont = document_pb2.OperationContainer()
        op_cont.index.CopyFrom(document_pb2.IndexOperation())
        for doc in doc_list:
            requestBody = document_pb2.BulkRequestBody()
            requestBody.object = doc.encode('utf-8')
            requestBody.operation_container.CopyFrom(op_cont)
            request.request_body.append(requestBody)
        return request

    """
    Parse stats from protobuf response.
    Consumed from params dictionary:
    * ``index``: index name
    * ``bulk-size``: documents per bulk request
    * ``unit``: in the case of bulk always 'ops'
    * ``detailed-results``: gRPC/Protobuf does not support detailed results at this time.
    """
    @staticmethod
    def build_stats(response : document_pb2.BulkResponse, params):
        if params.get("detailed-results"):
            raise Exception("Detailed results not supported for gRPC bulk requests")

        took = None
        error_count = 0
        success_count = 0
        if response.errors:
            error_count = params.get("bulk-size")
        else:
            took = response.took
            for item in response.items:
                if item.index.status > 299:
                    error_count += 1
                else:
                    success_count += 1

        meta_data = {
            "index": params.get("index"),
            "weight": params.get("bulk-size"),
            "unit": params.get("unit"),
            "took": took,
            "success": error_count == 0,
            "success-count": success_count,
            "error-count": error_count,
        }

        if error_count > 0:
            meta_data["error-type"] = "bulk"

        return meta_data