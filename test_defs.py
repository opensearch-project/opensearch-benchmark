from opensearch.protos.schemas import search_pb2
from opensearch.protos.schemas import common_pb2

vecs = [0.27278033, 0.51835215, -0.5857965, -0.07444, -0.27410728, 0.47767788, 0.30584392, -0.3112377, 0.13693847, 0.606695, -0.5558672, -0.58814126, -0.4316477, 0.5574045, -0.050333954, 0.020593066, 0.16764367, -0.112966135, -0.06999091, 0.36053005, -0.12604061, 0.7064357, -0.002698024, 0.07906186, -0.20551439, 0.5178902, -0.5403498, 0.14383093, -0.4718177, 0.16099364, 0.8372729, 0.088518314, -0.114860766, 0.3085837, -0.17499103, 0.37727773, 0.06619621, -0.06620696, -0.5667202, 1.0093662, 0.2882059, 0.07276506, 0.53782654, -0.011985853, 0.14079447, -0.24642251, 0.18002501, 0.023277052, -0.29374573, -0.53636354, -0.01768193, -0.6220148, -0.3439442, 0.30570453, -0.34143242, 0.06982459, 0.012607228, 0.33195263, 0.70104516, -0.1761785, 0.17121466, 0.18323132, -0.3522332, -0.28332865, 0.03734721, 0.18367398, -0.13212222, -0.07740674, 0.45408446, 0.038001493, -0.23421039, 0.68737054, -0.05750448, -0.06301772, 0.12623227, -0.45169166, -0.08787447, 0.35677314, 0.5323042, 0.056320306, 0.5703503, -0.1837923, -0.1412329, 0.1976004, 0.026460474, 0.37075898, 0.0083719315, 0.21027826, 0.3223662, 0.8550175, -0.23026448, 0.29023835, 0.016918905, -0.042846795, 0.30145866, 0.19485378, -0.08710589, -0.2743817, 0.33218256, -0.48350108]

query = common_pb2.KnnQuery(
            field="target_field",
            vector=vecs,
            k=100
        )

search_req = search_pb2.SearchRequest(
        request_body=search_pb2.SearchRequestBody(
            query = common_pb2.QueryContainer(
                knn=query
            )
        ),
        index="index",
        size=5
)





#         {
#   'index': 'target_index', 
#   'type': None,
#   'cache': None,
#   'detailed-results': True, 
#   'calculate-recall': True,
#   'request-params': {
#     '_source': 'false', 
#     'allow_partial_search_results': 'false'
#   }, 
#   'response-compression-enabled': True, 
#   'body': {
#     'docvalue_fields': ['_id'], 
#     'stored_fields': '_none_', 
#     'size': 100, 
#     'query': {
#       'knn': {
#         'target_field': {
#           'vector': array([ <BIG FLOAT ARRAY> ], dtype=float32),
#           'k': 100
#         }
#       }
#     }
#   }, 
#   'request-timeout': None, 
#   'headers': None, 
#   'opaque-id': None, 
#   'k': 100, 
#   'operation-type': 'vector-search', 
#   'id-field-name': '', 
#   'filter_type': {}, 
#   'filter_body': {}, 
#   'neighbors': [< A BUNCH OF LONGS 940591 >], 
#   'num_clients': 1, 
#   'num_cores': 10
#   }