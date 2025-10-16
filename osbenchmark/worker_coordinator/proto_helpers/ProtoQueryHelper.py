from numpy.lib.utils import source
from opensearch.protobufs.schemas import search_pb2
from opensearch.protobufs.schemas import common_pb2

def _get_relation(relation):
    match relation:
        case 0:
            return "TOTAL_HITS_RELATION_UNSPECIFIED"
        case 1:
            return "TOTAL_HITS_RELATION_EQ"
        case 2:
            return "TOTAL_HITS_RELATION_GTE"
        case _:
            return "TOTAL_HITS_RELATION_UNSET"

def _get_terms_dict(query):
    terms = {}
    for key, value in query.items():
        terms[key] = []
        if isinstance(value, list):
            terms[key].extend(value)
        elif isinstance(value, dict):
            for ignore, term_value in value.items():
                terms[key].append(term_value)
        else:
            raise Exception("Error parsing query - Term(s) are neither list nor dictionary: " + str(query))
    return terms

class ProtoQueryHelper:
    """
    Helper methods to build a protobuf query from OSB params dictionary.
    Supported protobuf types for this runner:
    - match all query
    - term query
    - terms query
    """

    """
    Parse term query into `common_pb2.TermQuery` protobuf.
    Term query supports a single term on single field.
    """
    @staticmethod
    def term_query_to_proto(query):
        term = _get_terms_dict(query)
        if len(term.keys()) > 1:
            raise Exception("Error parsing query - Term query contains multiple distinct fields: " + str(query))
        if len(term.values()) > 1:
            raise Exception("Error parsing query - Term query contains multiple terms: " + str(query))

        # Term query body gives field/value as lists
        term_field = next(iter(term.keys()))
        term_value = next(iter(term[term_field]))

        if type(term_value) is not str:
            raise Exception("Error parsing term query - Type [" + type(term_value) + "] is not supported.")

        f_val = common_pb2.FieldValue(string=term_value)
        return common_pb2.TermQuery(
            field=term_field,
            value=f_val
        )

    """
    Parse a query body into the corresponding protobuf type.
    Exceptions are thrown for unsupported query types.
    (Note that gRPC/protobuf API coverage is not comprehensive) 
    """
    @staticmethod
    def query_body_to_proto(body):
        query_body = body.get("query")
        for key, value in query_body.items():
            if key == "match_all":
                return common_pb2.QueryContainer(
                    match_all=common_pb2.MatchAllQuery()
                )
            if key == "term":
                return common_pb2.QueryContainer(
                    term=ProtoQueryHelper.term_query_to_proto(query_body.get("term"))
                )
        raise Exception("Unsupported query type: " + str(query_body))

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
        body = params.get("body")
        size = body.get("size") if "size" in body else None
        source = body.get("_source") if "_source" in body else False
        index = [params.get("index")]
        source_config = common_pb2.SourceConfigParam(bool=source)
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

    """
    Build protobuf SearchRequest for vector search workload.
    Vector search requests have a slightly different structure and provide additional params 
    outside of the query body.
    * ``body``: knn query body
    * ``index``: index name
    * ``request-timeout``: request timeout
    * ``cache``: enabled request cache
    * ``request-params``: vector search lists _source here
    """
    @staticmethod
    def build_vector_search_proto_request(params):
        if params.get("detailed-results"):
            raise NotImplementedError("Detailed results not supported for gRPC/protobuf vector search")
        if params.get("calculate-recall"):
            raise NotImplementedError("Recall calculations not supported for gRPC/protobuf vector search")
        if params.get("response-compression-enabled"):
            raise NotImplementedError("Compression not supported for gRPC/protobuf transport")

        body = params.get("body")
        size = body.get("size") if "size" in body else None
        request_params = params.get("request-params")
        index = [params.get("index")]
        source_config = common_pb2.SourceConfigParam(bool=request_params.get("source"))
        timeout = None if params.get("request-timeout") is None else str(params.get("request-timeout")) + "ms"

        if isinstance(params.get("cache"), bool):
            cache = params.get("cache")
        elif isinstance(params.get("cache"), str):
            cache = params.get("cache").lower() == "true"
        else:
            cache = None

        """
        Parse knn query into `common_pb2.KnnQuery` protobuf.
        """
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
                size = size
            ),
            index=index,
            x_source=source_config,
            request_cache=cache
        )

    """
    Parse stats from protobuf response.
    Consumed from params dictionary:
    * ``detailed-results``: return detailed results, hits, took, hits_relation
    """
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
                "hits_relation": _get_relation(response.hits.total.total_hits.relation),
                "timed_out": response.timed_out,
                "took": response.took,
            }

        return {
            "weight": 1,
            "unit": "ops",
            "success": True
        }
