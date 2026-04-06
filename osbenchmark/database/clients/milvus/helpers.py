# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Milvus helper functions for OpenSearch Benchmark.

Pure functions for schema translation, response conversion, and
parameter mapping. No network calls — all I/O is in milvus.py.
"""

import logging

logger = logging.getLogger(__name__)

# OpenSearch distance metrics -> Milvus metric types
METRIC_TYPE_MAP = {
    "cosinesimil": "COSINE",
    "l2": "L2",
    "innerproduct": "IP",
    "ip": "IP",
    "cosine": "COSINE",
    "angular": "COSINE",
    "COSINE": "COSINE",
    "L2": "L2",
    "IP": "IP",
}


def get_metric_type(space_type):
    """Convert OpenSearch space_type to Milvus metric type."""
    return METRIC_TYPE_MAP.get(space_type, "COSINE")


def build_collection_schema(client, params, client_options=None):
    """Build a Milvus CollectionSchema from vectorsearch workload params.

    Args:
        client: MilvusDatabaseClient instance (exposes create_schema/prepare_index_params)
        params: Workload params dict
        client_options: Client options dict

    Returns:
        tuple: (schema, index_params, collection_name)
    """
    from pymilvus import DataType  # pylint: disable=import-outside-toplevel,import-error

    client_options = client_options or {}
    dimension = int(params.get("target_index_dimension", 768))
    space_type = (client_options.get("metric_type")
                  or client_options.get("space_type")
                  or params.get("target_index_space_type", "cosinesimil"))
    metric_type = get_metric_type(space_type)
    vector_field = params.get("target_field_name", "embedding")
    collection_name = params.get("target_index_name", "target_index")

    hnsw_m = int(params.get("hnsw_m", client_options.get("hnsw_m", 16)))
    ef_construction = int(params.get(
        "hnsw_ef_construction",
        client_options.get("hnsw_ef_construction", 200)
    ))
    index_type = client_options.get("index_type", "HNSW").upper()

    schema = client.create_schema()
    schema.add_field(
        field_name="doc_id",
        datatype=DataType.INT64,
        is_primary=True,
        auto_id=False,
    )
    schema.add_field(
        field_name=vector_field,
        datatype=DataType.FLOAT_VECTOR,
        dim=dimension,
    )

    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name=vector_field,
        index_type=index_type,
        metric_type=metric_type,
        params={"M": hnsw_m, "efConstruction": ef_construction},
    )

    return schema, index_params, collection_name


def build_search_params(params, client_options=None):
    """Build Milvus search parameters from workload params.

    metric_type is intentionally omitted — the server infers it from the index.
    """
    client_options = client_options or {}
    k = int(params.get("k", params.get("query_k", 100)))
    index_type = client_options.get("index_type", "HNSW").upper()

    ef_search = int(
        client_options.get("hnsw_ef_search")
        or params.get("hnsw_ef_search")
        or max(k, 256)
    )

    if index_type == "HNSW":
        search_params = {"params": {"ef": ef_search}}
    elif index_type.startswith("IVF"):
        nprobe = int(client_options.get("nprobe", params.get("nprobe", 128)))
        search_params = {"params": {"nprobe": nprobe}}
    elif index_type == "DISKANN":
        search_params = {"params": {"search_list": ef_search}}
    else:
        search_params = {"params": {}}

    return {"k": k, "search_params": search_params, "ef_search": ef_search}


def convert_milvus_search_response(results, collection_name="default"):
    """Convert pymilvus search results to OpenSearch-compatible format.

    pymilvus MilvusClient.search() returns List[List[Hit]].
    Hit fields are accessed by key: h["doc_id"], h["distance"].
    The primary key is accessed via its FIELD NAME ("doc_id"), not "id".
    """
    if not results or len(results) == 0:
        return {
            "took": 0,
            "timed_out": False,
            "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []},
        }

    hits_list = results[0]
    os_hits = []
    for hit in hits_list:
        os_hits.append({
            "_index": collection_name,
            "_id": str(hit.get("doc_id", hit.get("id", ""))),
            "_score": hit.get("distance", 0.0),
        })

    return {
        "took": 0,
        "timed_out": False,
        "hits": {
            "total": {"value": len(os_hits), "relation": "eq"},
            "hits": os_hits,
        },
    }


def parse_vector_body(body, vector_field="embedding"):
    """Parse vectorsearch workload body into documents for Milvus insert.

    Input: alternating action/doc pairs from BulkVectorDataSetParamSource:
        [{"index": {"_index": "idx", "_id": 0}}, {"embedding": [...]}, ...]

    Returns:
        tuple: (list of {"doc_id": int, "embedding": list}, index_name)
    """
    prepared = []
    index = None

    if not isinstance(body, list):
        return prepared, index

    i = 0
    while i < len(body) - 1:
        action_meta = body[i]
        doc_body = body[i + 1]
        if action_meta is None or doc_body is None:
            i += 2
            continue

        action_type = list(action_meta.keys())[0]
        meta = action_meta[action_type]
        raw_id = meta.get("_id", i // 2)
        try:
            doc_id = int(raw_id)
        except (ValueError, TypeError):
            raise ValueError(
                f"Milvus INT64 primary key requires integer _id, got: {raw_id!r}"
            )
        if index is None:
            index = meta.get("_index")

        doc = {"doc_id": doc_id}
        for k, v in doc_body.items():
            doc[k] = v.tolist() if hasattr(v, 'tolist') else v
        prepared.append(doc)
        i += 2

    return prepared, index


def calculate_topk_recall(predictions, neighbors, top_k):
    """Calculate recall@k by comparing predictions against ground truth neighbors.

    Both predictions and neighbors are coerced to strings to avoid type mismatch.
    Milvus returns INT64 IDs, HDF5 neighbors are numpy int64, and various code
    paths may deliver str, int, or np.int64. Coercing both to str eliminates
    this entire class of bugs.
    """
    if neighbors is None:
        return 0.0
    min_results = min(top_k, len(neighbors))
    truth_set = set(str(n) for n in neighbors[:min_results] if str(n) != "-1")
    if not truth_set:
        return 1.0
    correct = sum(1.0 for p in predictions[:min_results] if str(p) in truth_set)
    return correct / len(truth_set)
