from opensearch.protobufs.schemas import search_pb2
from opensearch.protobufs.schemas import common_pb2

# In some cases (KNN) we set stored fields explicitly to "_none_" to disable
# https://github.com/opensearch-project/OpenSearch/blob/3.3/server/src/main/java/org/opensearch/search/fetch/StoredFieldsContext.java#L59
STORED_FIELDS_NONE = "_none_"

def is_true(value):
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)

def get_relation(relation):
    match relation:
        case 0:
            return "TOTAL_HITS_RELATION_UNSPECIFIED"
        case 1:
            return "TOTAL_HITS_RELATION_EQ"
        case 2:
            return "TOTAL_HITS_RELATION_GTE"
        case _:
            return "TOTAL_HITS_RELATION_UNSET"

def get_terms_dict(query):
    terms = {}
    for key, value in query.items():
        terms[key] = []
        if isinstance(value, list):
            terms[key].extend(value)
        elif isinstance(value, dict):
            for _, term_value in value.items():
                terms[key].append(term_value)
        else:
            raise Exception("Error parsing query - Term(s) are neither list nor dictionary: " + str(query))
    return terms

class ProtoQueryHelper:
    # Parse term query into common_pb2.TermQuery protobuf.
    # Term query supports a single term on single field.
    @staticmethod
    def term_query_to_proto(query):
        term = get_terms_dict(query)
        if len(term.keys()) > 1:
            raise Exception("Error parsing query - Term query contains multiple distinct fields: " + str(query))
        if len(term.values()) > 1:
            raise Exception("Error parsing query - Term query contains multiple terms: " + str(query))

        # Term query body gives field/value as lists
        term_field = next(iter(term.keys()))
        term_value = next(iter(term[term_field]))

        if not isinstance(term_value, str):
            raise Exception(f"Error parsing query - Term query field value is not a supported type: {term_value} (type: {type(term_value).__name__})")

        f_val = common_pb2.FieldValue(string=term_value)
        return common_pb2.TermQuery(
            field=term_field,
            value=f_val
        )

    # Parse a query body into the corresponding protobuf type.
    # Exceptions are thrown for unsupported query types.
    # (Note that gRPC/protobuf API coverage is not comprehensive)
    @staticmethod
    def query_body_to_proto(body):
        query_body = body.get("query")
        for key, _ in query_body.items():
            if key == "match_all":
                return common_pb2.QueryContainer(
                    match_all=common_pb2.MatchAllQuery()
                )
            if key == "term":
                return common_pb2.QueryContainer(
                    term=ProtoQueryHelper.term_query_to_proto(query_body.get("term"))
                )
        raise Exception("Unsupported query type: " + str(query_body))

    # Build protobuf SearchRequest.
    # Consumed from params dictionary:
    # ``body``: query body as loaded from workload - Contains `_size` and `source`
    # ``index``: index name
    # ``request-timeout``: request timeout
    # ``cache``: enabled request cache
    @staticmethod
    def build_proto_request(params):
        body = params.get("body")
        size = body.get("size") if "size" in body else None
        fetch_source = is_true(body.get("_source"))
        source_config = common_pb2.SourceConfigParam(bool=fetch_source)
        index = [params.get("index")]
        timeout = None if params.get("request-timeout") is None else str(params.get("request-timeout")) + "ms"

        if isinstance(params.get("cache"), bool):
            cache = params.get("cache")
        elif isinstance(params.get("cache"), str):
            cache = params.get("cache").lower() == "true"
        else:
            cache = None

        return search_pb2.SearchRequest(
            request_body=search_pb2.SearchRequestBody(
                query=ProtoQueryHelper.query_body_to_proto(body),
                timeout=timeout,
                size = size
            ),
            index=index,
            x_source=source_config,
            request_cache=cache
        )

    # Build protobuf SearchRequest for vector search workload.
    # Vector search requests have a slightly different structure and provide additional params
    # outside the query body.
    # ``body``: knn query body
    # ``index``: index name
    # ``request-timeout``: request timeout
    # ``cache``: enabled request cache
    # ``request-params``: vector search lists _source here
    @staticmethod
    def build_vector_search_proto_request(params):
        if is_true(params.get("detailed-results")):
            raise NotImplementedError("Detailed results not supported for gRPC/protobuf vector search")
        if is_true(params.get("calculate-recall")) or params.get("id-field-name"):
            raise NotImplementedError("Recall calculations not supported for gRPC/protobuf vector search")
        if is_true(params.get("response-compression-enabled")):
            raise NotImplementedError("Compression not supported for gRPC/protobuf transport")
        if params.get("type"):
            raise NotImplementedError("Doc type not supported for knn query type")
        if params.get("filter_body") or params.get("filter_type"):
            raise NotImplementedError("Filter options not supported for gRPC/protobuf vector search")

        index = [params.get("index")]
        body = params.get("body")
        doc_value_fields = body.get("docvalue_fields")
        size = body.get("size") if "size" in body else None
        request_params = params.get("request-params")
        fetch_source = is_true(request_params.get("_source"))
        profile_query = is_true(params.get("profile_query"))
        partial_results = is_true(request_params.get("allow_partial_search_results"))
        source_config = common_pb2.SourceConfigParam(bool=fetch_source)
        timeout = params.get("request-timeout")

        stored_fields = body.get("stored_fields")
        if stored_fields is None or stored_fields == STORED_FIELDS_NONE:
            stored_fields = [STORED_FIELDS_NONE]
        elif not isinstance(stored_fields, list):
            raise Exception("Error parsing query params - Stored fields must be a list")

        if isinstance(params.get("cache"), bool):
            cache = params.get("cache")
        elif isinstance(params.get("cache"), str):
            cache = params.get("cache").lower() == "true"
        else:
            cache = None

        # Parse knn query into `common_pb2.KnnQuery` protobuf.
        def knn_query_to_proto(query) -> common_pb2.KnnQuery:
            knn_query = query.get("knn")
            target_field_key = next(iter(knn_query.keys()))
            vector = knn_query[target_field_key].get("vector")
            k = knn_query[target_field_key].get("k")
            return common_pb2.KnnQuery(
                field=target_field_key,
                vector=vector,
                k=k
            )

        knn_query_proto = knn_query_to_proto(body.get("query"))
        return search_pb2.SearchRequest(
            request_body=search_pb2.SearchRequestBody(
                query=common_pb2.QueryContainer(knn=knn_query_proto),
                timeout=timeout,
                profile=profile_query,
                size = size
            ),
            index=index,
            x_source=source_config,
            request_cache=cache,
            allow_partial_search_results=partial_results,
            docvalue_fields=doc_value_fields,
            stored_fields=stored_fields
        )

    # Parse stats from protobuf response.
    # ``detailed-results``: return detailed results, hits, took, hits_relation
    @staticmethod
    def build_stats(response, params):
        if not isinstance(response, search_pb2.SearchResponse):
            raise Exception("Unknown response proto: " + response)

        if params.get("detailed-results"):
            return {
                "weight": 1,
                "unit": "ops",
                "success": True,
                "hits": response.hits.total.total_hits.value,
                "hits_relation": get_relation(response.hits.total.total_hits.relation),
                "timed_out": response.timed_out,
                "took": response.took,
            }

        return {
            "weight": 1,
            "unit": "ops",
            "success": True
        }
