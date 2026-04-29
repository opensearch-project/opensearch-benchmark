import numpy as np
from opensearch.protobufs.schemas import common_pb2

def _parse_docs_from_body(body):
    index_op_lines = body.decode('utf-8').split('\n')
    doc_list = []
    for doc in index_op_lines[1::2]:
        doc_list.append(doc)
    return doc_list

def _build_float_binary_le(vector):
    """Convert a numpy float32 vector to a FloatBinaryLE protobuf message."""
    arr = np.asarray(vector, dtype=np.float32)
    if not arr.dtype.byteorder == '=' or not np.little_endian:
        arr = arr.astype('<f4')
    return common_pb2.FloatBinaryLE(bytes_le=arr.tobytes(), dimension=len(arr))

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
        request = common_pb2.BulkRequest()
        request.index = index
        # All bulk requests here are index ops
        op_container = common_pb2.OperationContainer()
        op_container.index.CopyFrom(common_pb2.IndexOperation())
        for doc in doc_list:
            request_body = common_pb2.BulkRequestBody()
            request_body.object = doc.encode('utf-8')
            request_body.operation_container.CopyFrom(op_container)
            request.bulk_request_body.append(request_body)
        return request

    # Build protobuf BulkRequest with extra_field_values for vector data.
    # Consumed from params dictionary:
    # * ``vectors``: list of numpy float32 arrays (one per document)
    # * ``index``: index name
    # * ``field``: vector field name (used as key in extra_field_values map)
    @staticmethod
    def build_proto_vector_request(params):
        index = params.get("index")
        vectors = params.get("vectors")
        field = params.get("field")
        request = common_pb2.BulkRequest()
        request.index = index
        op_container = common_pb2.OperationContainer()
        op_container.index.CopyFrom(common_pb2.IndexOperation())
        for vec in vectors:
            request_body = common_pb2.BulkRequestBody()
            request_body.object = b'{}'
            request_body.operation_container.CopyFrom(op_container)
            float_binary_le = _build_float_binary_le(vec)
            float_array_value = common_pb2.FloatArrayValue(binary_le=float_binary_le)
            binary_field_value = common_pb2.BinaryFieldValue(float_array_value=float_array_value)
            request_body.extra_field_values[field].CopyFrom(binary_field_value)
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
