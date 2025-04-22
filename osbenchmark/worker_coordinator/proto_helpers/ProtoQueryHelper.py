from opensearch_protos.protos.schemas import search_pb2
from opensearch_protos.protos.schemas import common_pb2

def _parse_terms_from_query(query):
    terms_map = common_pb2.ObjectMap()
    for key, value in query.items():
        obj_map_val = common_pb2.ObjectMap.Value(string=value.get("value"))
        terms_map.FieldsEntry(key=key, value=obj_map_val)
    return common_pb2.FieldValue(object_map=terms_map)

def _parse_query_from_body(body):
    query_body = body.get("query")
    for key, value in query_body.items():
        if key == "match_all":
            return common_pb2.QueryContainer(match_all=common_pb2.MatchAllQuery())
        if key == "term":
            search_terms = _parse_terms_from_query(query_body.get("term"))
            return common_pb2.QueryContainer(term={"term_q": common_pb2.TermQuery(value=search_terms)})
    raise Exception("Unknown query type: " + str(query_body))

class ProtoQueryHelper:
    """
    Helper methods to build a protobuf query from OSB params dictionary.
    Supported protobuf types:
    match all query, term query
    """

    """
    Build protobuf SearchRequest.
    Consumed from params dictionary:
    * ``body``: query body as loaded from workload
    * ``index``: index name
    * ``source_config``: whether to include source in response
    * ``request-timeout``: request timeout
    * ``results-per-page``: size of hits returned
    * ``cache``: enabled request cache
    """
    @staticmethod
    def build_proto_request(params):
        body = params.get("body")
        index = [params.get("index")]
        source_config_bool = params.get("source_config") == 'True' or params.get("source_config") == 'true'
        source_config = common_pb2.SourceConfigParam(bool_value=source_config_bool)
        timeout = str(params.get("request-timeout")) + "ms" # OSB timeout always specified in ms
        cache = False if params.get("cache") is None else True if params.get("cache").lower() == "true" else False
        size = params.get("results-per-page")

        return search_pb2.SearchRequest(
            request_body=search_pb2.SearchRequestBody(query=_parse_query_from_body(body)),
            index=index,
            source=source_config,
            timeout=timeout,
            request_cache=cache,
            size=size
        )

    """
    Parse stats from protobuf response.
    Consumed from params dictionary:
    * ``detailed-results``: return detailed results, hits, took, hits_relation
    """
    @staticmethod
    def build_simple_stats(response, params):
        which_field = response.WhichOneof('response')
        if which_field == 'error_4xx_response' or which_field == 'error_5xx_response':
            raise Exception("Server responded with error: " + str(which_field))

        if not isinstance(response.response_body, search_pb2.ResponseBody):
            raise Exception("Unknown response proto: " + str(type(response)))

        print("relation: " + str(response.response_body.hits.total.total_hits.relation))
        print("val: " + str(response.response_body.hits.total.total_hits.value))
        exit()

        hits_total = response.hits.total.value
        hits_relation_total = response.hits.relation.total

        if params.get("detailed-results"):
            return {
                "weight": 1,
                "unit": "ops",
                "success": True,
                "hits": response.hits.total,
                "hits_relation": response.hits_relation,
                "timed_out": response.timed_out,
                "took": response.took,
            }

        return {
            "weight": 1,
            "unit": "ops",
            "success": True
        }