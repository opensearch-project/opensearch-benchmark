import json

import cbor2
from opensearch.protobufs.schemas import common_pb2

SUPPORTED_FORMATS = ("json", "cbor")

def _parse_docs_from_body(body):
    index_op_lines = body.decode('utf-8').split('\n')
    doc_list = []
    for doc in index_op_lines[1::2]:
        doc_list.append(doc)
    return doc_list

def _serialize_doc(doc_str, doc_format):
    if doc_format == "cbor":
        return cbor2.dumps(json.loads(doc_str))
    return doc_str.encode('utf-8')

class ProtoBulkHelper:
    # Build protobuf BulkRequest.
    # Consumed from params dictionary:
    # * ``body``: JSON body of bulk ingest request
    # * ``index``: index name
    # * ``document-format``: serialization format for documents ("json" or "cbor", default "json")
    @staticmethod
    def build_proto_request(params):
        index = params.get("index")
        doc_format = params.get("document-format", "json")
        if doc_format not in SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported document-format [{doc_format}]. Supported: {SUPPORTED_FORMATS}")

        request = common_pb2.BulkRequest()
        request.index = index
        op_container = common_pb2.OperationContainer()
        op_container.index.CopyFrom(common_pb2.IndexOperation())

        for doc in _parse_docs_from_body(params.get("body")):
            request_body = common_pb2.BulkRequestBody()
            request_body.object = _serialize_doc(doc, doc_format)
            request_body.operation_container.CopyFrom(op_container)
            request.bulk_request_body.append(request_body)

        return request

    # Parse stats from protobuf response.
    # Consumed from params dictionary:
    # ``index``: index name
    # ``bulk-size``: documents per bulk request
    # ``unit``: in the case of bulk always 'ops'
    # ``detailed-results``: gRPC/Protobuf does not support detailed results at this time.
    @staticmethod
    def build_stats(response : common_pb2.BulkResponse, params):
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
