import json

import cbor2
from opensearch.protobufs.schemas import common_pb2

SUPPORTED_FORMATS = ("json", "cbor")


def _serialize_doc_dict(doc_dict, doc_format):
    """Serialize a Python dict to bytes in the specified format."""
    if doc_format == "cbor":
        return cbor2.dumps(doc_dict)
    return json.dumps(doc_dict).encode('utf-8')


class ProtoVectorBulkHelper:
    """Builds protobuf BulkRequests from dict-list bodies (vector data set format).

    Unlike ProtoBulkHelper which handles NDJSON bytes bodies, this helper
    handles the list-of-dicts body format produced by BulkVectorsFromDataSetParamSource:
        [action_dict, doc_dict, action_dict, doc_dict, ...]
    """

    @staticmethod
    def build_proto_request(params):
        """Build a protobuf BulkRequest from vector data set params.

        Consumed from params dictionary:
        * ``body``: list of alternating [action_dict, doc_dict, ...] pairs
        * ``index``: index name
        * ``document-format``: "json" or "cbor" (default "json")
        """
        index = params.get("index")
        doc_format = params.get("document-format", "json")
        if doc_format not in SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported document-format [{doc_format}]. Supported: {SUPPORTED_FORMATS}")

        body = params.get("body")

        # Extract index from action metadata if not directly in params
        if not index and body and len(body) > 0:
            action = body[0]
            for op_type in ("index", "create"):
                if op_type in action and "_index" in action[op_type]:
                    index = action[op_type]["_index"]
                    break

        request = common_pb2.BulkRequest()
        if index:
            request.index = index

        op_container = common_pb2.OperationContainer()
        op_container.index.CopyFrom(common_pb2.IndexOperation())

        # body is [action, doc, action, doc, ...] — take every other element starting at index 1
        for doc_dict in body[1::2]:
            request_body = common_pb2.BulkRequestBody()
            request_body.object = _serialize_doc_dict(doc_dict, doc_format)
            request_body.operation_container.CopyFrom(op_container)
            request.bulk_request_body.append(request_body)

        return request

    @staticmethod
    def build_stats(response, params):
        if params.get("detailed-results"):
            raise Exception("Detailed results not supported for gRPC bulk requests")

        took = None
        error_count = 0
        success_count = 0
        if response.errors:
            error_count = params.get("size", params.get("bulk-size", 0))
        else:
            took = response.took
            for item in response.items:
                if item.index.status > 299:
                    error_count += 1
                else:
                    success_count += 1

        meta_data = {
            "index": params.get("index"),
            "weight": params.get("size", params.get("bulk-size", 0)),
            "unit": params.get("unit", "docs"),
            "took": took,
            "success": error_count == 0,
            "success-count": success_count,
            "error-count": error_count,
        }

        if error_count > 0:
            meta_data["error-type"] = "bulk"

        return meta_data
