from opensearch.protobufs.schemas import search_pb2
from opensearch.protobufs.schemas import common_pb2

def _get_relation(relation):
    if relation == 0:
        return "TOTAL_HITS_RELATION_UNSPECIFIED"
    elif relation == 1:
        return "TOTAL_HITS_RELATION_EQ"
    elif relation == 2:
        return "TOTAL_HITS_RELATION_GTE"
    else:
        return "TOTAL_HITS_RELATION_UNSET"

def _get_terms_dict(query):
    terms = {}
    for key, value in query.items():
        terms[key] = []
    for key, value in query.items():
        if type(value) is list:
            for item in value:
                terms[key].append(item)
        elif type(value) is dict:
            for ignore, term_value in value.items():
                terms[key].append(term_value)
        else:
            raise Exception("Error parsing query - Term(s) are neither list nor dictionary: " + str(query))
    return terms

"""
Parse term query into `common_pb2.TermQuery` protobuf.
Term query supports a single term on single field.
"""
def _parse_term_from_query(query):
    term = _get_terms_dict(query)
    if len(term.keys()) > 1:
        raise Exception("Error parsing query - Term query contains multiple distinct fields: " + str(query))
    if len(term.values()) > 1:
        raise Exception("Error parsing query - Term query contains multiple terms: " + str(query))

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
Parse terms query into `common_pb2.TermsQuery` protobuf.
Terms query supports multiple terms for a single field.
"""
def _parse_terms_from_query(query):
    terms = _get_terms_dict(query)
    if len(terms.keys()) > 1:
        raise Exception("Error parsing query - Term query contains multiple distinct fields: " + str(query))

    term_field = next(iter(terms.keys()))
    terms_array = common_pb2.StringArray(string_array=terms[term_field])
    terms_lookup_map = common_pb2.TermsLookupFieldStringArrayMap(string_array=terms_array)

    return common_pb2.TermsQueryField(
        terms_lookup_field_string_array_map={term_field: terms_lookup_map}
    )

def _parse_query_from_body(body):
    query_body = body.get("query")
    for key, value in query_body.items():
        if key == "match_all":
            return common_pb2.QueryContainer(match_all=common_pb2.MatchAllQuery())
        if key == "term":
            return common_pb2.QueryContainer(term=_parse_term_from_query(query_body.get("term")))
        if key == "terms":
            return common_pb2.QueryContainer(terms=_parse_terms_from_query(query_body.get("terms")))
    raise Exception("Unsupported query type: " + str(query_body))

class ProtoQueryHelper:
    """
    Helper methods to build a protobuf query from OSB params dictionary.
    Supported protobuf types for this runner:
    - match all query
    - term query
    - terms query
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
        body = params.get("body")
        size = body.get("size") if "size" in body else None
        source = body.get("_source") if "_source" in body else False
        index = [params.get("index")]
        source_config = common_pb2.SourceConfigParam(bool_value=source)
        timeout = None if params.get("request-timeout") is None else str(params.get("request-timeout")) + "ms" # OSB timeout always specified in ms
        cache = False if params.get("cache") is None else True if params.get("cache").lower() == "true" else False

        return search_pb2.SearchRequest(
            request_body=search_pb2.SearchRequestBody(query=_parse_query_from_body(body)),
            index=index,
            x_source=source_config,
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
    def build_stats(response, params):
        which_field = response.WhichOneof('response')
        if which_field == 'error_4xx_response' or which_field == 'error_5xx_response':
            raise Exception("Server responded with error: " + str(which_field))

        if not isinstance(response.response_body, search_pb2.ResponseBody):
            raise Exception("Unknown response proto: " + str(type(response)))

        if params.get("detailed-results"):
            return {
                "weight": 1,
                "unit": "ops",
                "success": True,
                "hits": response.response_body.hits.total.total_hits.value,
                "hits_relation": _get_relation(response.response_body.hits.total.total_hits.relation),
                "timed_out": response.response_body.timed_out,
                "took": response.response_body.took,
            }

        return {
            "weight": 1,
            "unit": "ops",
            "success": True
        }