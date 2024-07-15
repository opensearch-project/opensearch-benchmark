from dataclasses import dataclass, field
from typing import List

@dataclass
class Index:
    name: str
    settings_and_mappings: dict
    document_frequency: int
    limit_documents: int

# @dataclass
# class Corpus:
#     index_name: str
#     document_frequency: int
#     limit_documents: int

@dataclass
class CustomWorkload:
    workload_name: str
    root_path: str
    indices: List[Index]
    failed_indices: List[Index]
    corpora: List[dict]
    queries: List[str]
    workload_path: str
    operations_path: str
    test_procedures_path: str
