from dataclasses import dataclass, field
from typing import List

@dataclass
class Index:
    name: str = None
    document_frequency: int = 1
    number_of_docs: int = None
    settings_and_mappings: dict = field(default_factory=dict)

@dataclass
class CustomWorkload:
    workload_name: str = None
    root_path: str = None
    workload_path: str = None
    operations_path: str = None
    test_procedures_path: str = None
    indices: List[Index] = field(default_factory=list)
    extracted_indices: List[str] = field(default_factory=list)
    failed_indices: List[str] = field(default_factory=list)
    corpora: List[dict] = field(default_factory=list)
    queries: List[dict] = field(default_factory=list)
