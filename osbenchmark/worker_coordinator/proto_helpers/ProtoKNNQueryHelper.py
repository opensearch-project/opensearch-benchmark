from opensearch.protobufs.schemas import search_pb2
from opensearch.protobufs.schemas import common_pb2

from osbenchmark.worker_coordinator.proto_helpers.ProtoQueryHelper import _get_relation


class ProtoKNNQueryHelper:
    """
    Helper methods to build a protobuf query from OSB params dictionary.
    Supported protobuf types for this runner:
    knn query
    """

    """
    Build protobuf SearchRequest.
    Consumed from params dictionary:
    * ``body``: query body as loaded from workload - Contains `_size` and `source`
    * ``index``: index name
    * ``request-timeout``: request timeout
    * ``cache``: enabled request cache
    """
    @staticmethod
    def build_proto_request(params):
        index = [params.get("index")]
        type = params.get("type")
        cache = params.get("cache")
        detailed_results = params.get("detailed-results")
        calc_recall = params.get("calculate-recall")
        resp_compression = params.get("response-compression-enabled")

        req_params = params.get("request-params")
        source = None
        allow_partial_search_results = None
        if req_params is not None:
            source = req_params.get("_source") if "_source" in req_params else None
            allow_partial_search_results = req_params.get("allow_partial_search_results") if "allow_partial_search_results" in req_params else None

        body = params.get("body")
        docvalue_fields = body.get("docvalue_fields") if "docvalue_fields" in body else None
        stored_fields = body.get("stored_fields") if "stored_fields" in body else None
        size = body.get("size") if "size" in body else None

        query = body.get("query")
        knn_query = query.get("knn")
        target_field = knn_query.get("target_field")
        vector = target_field.get("vector")
        k = target_field.get("k")

        knn_query_proto = common_pb2.KnnQuery(
            field="target_field",
            vector=vector,
            k=k
        )

        search_req = search_pb2.SearchRequest(
            request_body=search_pb2.SearchRequestBody(
                query=common_pb2.QueryContainer(
                    knn=knn_query_proto
                )
            ),
            index=index,
            source=source,
            request_cache=cache,
            size=size
        )

        return search_req
