from opensearch.protobufs.schemas import document_pb2

def _parse_docs_from_body(body):
    index_op_lines = body.decode('utf-8').split('\n')
    doc_list = []
    for doc in index_op_lines[1::2]:
        doc_list.append(doc)
    return doc_list

class ProtoBulkHelper:
    # Build protobuf SearchRequest.
    # Consumed from params dictionary:
    # * ``body``: JSON body of bulk ingest request
    # * ``index``: index name
    @staticmethod
    def build_proto_request(params):
        index = params.get("index")
        body = params.get("body")
        doc_list = _parse_docs_from_body(body)
        request = document_pb2.BulkRequest()
        request.index = index
        # All bulk requests here are index ops
        op_container = document_pb2.OperationContainer()
        op_container.index.CopyFrom(document_pb2.IndexOperation())
        for doc in doc_list:
            request_body = document_pb2.BulkRequestBody()
            request_body.object = doc.encode('utf-8')
            request_body.operation_container.CopyFrom(op_container)
            request.request_body.append(request_body)
        return request

    # Parse stats from protobuf response.
    # Consumed from params dictionary:
    # ``index``: index name
    # ``bulk-size``: documents per bulk request
    # ``unit``: in the case of bulk always 'ops'
    # ``detailed-results``: gRPC/Protobuf does not support detailed results at this time.
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
                # status field mirrors http code conventions
                # https://github.com/opensearch-project/opensearch-protobufs/blob/b6f889416da83b7dc4a0408347965e7820bd61d0/protos/schemas/document.proto#L217-L219
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
