from dataclasses import dataclass, field
from typing import List

@dataclass
class Index:
    name: str
    settings: dict
    mappings: dict
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
    failed_corpus: List[Index]
    queries: List[str]
    workload_path: str
    operations_path: str
    test_procedures_path: str

