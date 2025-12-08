# OpenSearch Benchmark: Vespa Integration Design Document

## 1. Architecture Overview

### 1.1 High-Level Design Pattern

OpenSearch Benchmark uses a **client factory pattern** with **backend detection** to route operations to different database systems. The Vespa integration extends this pattern without modifying the core workload or test execution logic.

`WorkerCoordinator`
```python
if "backend:vespa" in client_options:
    factory = VespaClientFactory()
    client = VespaAsyncClient()
else:
    factory = OsClientFactory()
    client = BenchmarkAsyncOpenSearch()
```
Runner Registry:
```
Default Runners:     Vespa Override:
  bulk → BulkIndex     bulk → VespaBulkIndex
  search → Query       search → VespaQuery
  ...                  vector-search → VespaVectorSearch
```

### 1.2 Integration Strategy

The Vespa integration follows an **adapter pattern** approach:

1. **Minimal Core Changes:** Only 3 integration points in OSB core code
2. **Backend Detection:** Client options contain `backend:vespa` flag
3. **Client Adapter:** VespaAsyncClient provides OpenSearch-compatible interface
4. **API Translation:** Converts OpenSearch operations to Vespa REST API calls
5. **Response Conversion:** Translates Vespa responses back to OpenSearch format
6. **Timing Integration:** Inherits RequestContextHolder mixin for timing

This approach allows OSB workloads to run unmodified against Vespa while maintaining full timing and metrics compatibility.

### 1.3 Key Design Principles

1. **Compatibility First:** Maintain OpenSearch API surface for workload compatibility
2. **Minimal Invasiveness:** Avoid modifying OSB core logic where possible
3. **Timing Accuracy:** Preserve OSB's 4-layer timing system for fair benchmarks
4. **Pragmatic Approach:** Focus on getting results rather than perfect abstractions
5. **Test-Driven:** Validate with both unit tests and full workload runs

---

## 2. Core Components

### 2.1 Component Overview

The Vespa integration consists of four main components:

| Component | File |  Purpose |
|-----------|------|-|
| **Client Factory & Client** | `vespa_client.py` | Creates Vespa clients, implements OpenSearch-compatible API |
| **Operation Runners** | `vespa_runners.py` | Adapts OSB operations to Vespa-specific implementations |
| **Integration Glue** | `vespa_integration.py` | Backend detection, utilities, workload adaptation |
| **Core Integration** | `worker_coordinator.py` | Backend routing logic (3 integration points) |

### 2.2 Client Factory and Async Client

**File:** `opensearch-benchmark/osbenchmark/vespa_client.py`

The client consists of `VespaClientFactory` (creates clients) and `VespaAsyncClient` (implements OpenSearch-compatible interface).

**Key Design:** Inherits `RequestContextHolder` mixin for timing integration.

### 2.3 Operation Runners

**File:** `opensearch-benchmark/osbenchmark/vespa_runners.py`

**Standard Pattern (all runners follow this):**

```python
class VespaRunner(Runner, Delegator):
    async def __call__(self, vespa_client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()  # Fallback timing
        try:
            result = await vespa_client.operation(...)
            return {"weight": count, "unit": "ops"}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
```

**Why Fallback Timing?** Ensures timing fields are always set, even if aiohttp trace hooks fail silently.
This was critical for resolving a `KeyError: 'request_start'` issue that resulted from Vespa's async HTTP API.

### 2.4 Integration Points in OSB Core

**File:** `opensearch-benchmark/osbenchmark/worker_coordinator/worker_coordinator.py`

Three minimal integration points enable Vespa support:

#### 1. Backend Detection at Startup (lines 649-656)

```python
def receiveMsg_PrepareBenchmark(self, msg, sender):
    client_options = msg.config.opts("client", "options").default
    from osbenchmark import vespa_integration

    if vespa_integration.is_vespa_backend(client_options):
        from osbenchmark.vespa_client import VespaClientFactory
        client_factory = VespaClientFactory
    else:
        from osbenchmark.client import OsClientFactory
        client_factory = OsClientFactory
```

**Purpose:** Select appropriate factory at benchmark startup

#### 2. Custom Runner Override System (lines 1720-1738)

```python
def receiveMsg_WorkloadLoaded(self, msg, sender):
    # After workload plugins load
    if vespa_integration.is_vespa_backend(client_options):
        from osbenchmark.vespa_runners import VESPA_RUNNERS
        for op_type, vespa_runner in VESPA_RUNNERS.items():
            runner.register_runner(op_type, vespa_runner, async_runner=True)
```

**Purpose:** Override workload-defined runners with Vespa versions

**Why Critical:** Some workloads define custom operations using OpenSearch-specific APIs (e.g., `warmup-knn-indices` uses transport API). This allows replacing them with Vespa implementations.

#### 3. Client Creation (lines 2228-2236)

```python
def _create_clients(self, cluster_settings):
    if vespa_integration.is_vespa_backend(cluster_options):
        rest_client_factory = vespa_integration.create_vespa_client_factory(
            cluster_hosts, cluster_options
        )
        opensearch[cluster_name] = rest_client_factory.create_async()
    else:
        rest_client_factory = OsClientFactory(cluster_hosts, cluster_options)
        opensearch[cluster_name] = rest_client_factory.create()
```

**Purpose:** Create actual client instances based on backend

**Summary of Integration Points:**

| Location | Purpose | Trigger | Action |
|----------|---------|---------|--------|
| `receiveMsg_PrepareBenchmark` | Select factory | Benchmark startup | Choose VespaClientFactory vs OsClientFactory |
| `receiveMsg_WorkloadLoaded` | Override runners | After workload loads | Replace operation runners with Vespa versions |
| `_create_clients` | Instantiate clients | Client creation | Create VespaAsyncClient vs AsyncOpenSearch |

**Design Philosophy:** Minimal touch points (only 3 locations), maximum compatibility with existing OSB infrastructure.

---

## 3. Recommendations for Future Backends

### 3.1 Step-by-Step Integration Guide

This section provides a blueprint for adding a new backend (e.g., Milvus) based on the Vespa experience.

#### Phase 1: Client Implementation

**Step 1: Create Client Factory** (`milvus_client.py`)

```python
class MilvusClientFactory:
    def __init__(self, hosts, client_options):
        self.hosts = hosts
        self.client_options = client_options

    def create_async(self):
        return MilvusAsyncClient(
            host=self.hosts[0]["host"],
            port=self.client_options.get("port", 19530),
            collection_name=self.client_options.get("collection_name", "vectors"),
            # ... other Milvus-specific options
        )
```

**Step 2: Create Async Client** (inherit `RequestContextHolder`)

```python
from pymilvus import connections, Collection
from osbenchmark.context import RequestContextHolder

class MilvusAsyncClient(RequestContextHolder):
    def __init__(self, host, port, collection_name):
        super().__init__()
        self.host = host
        self.port = port
        self.collection_name = collection_name
        self._collection = None

    async def _ensure_connection(self):
        """Ensure Milvus connection is established."""
        if self._collection is None:
            connections.connect(host=self.host, port=self.port)
            self._collection = Collection(self.collection_name)

    async def bulk(self, body, index=None, **kwargs):
        await self._ensure_connection()

        # Parse OpenSearch bulk format
        # Convert to Milvus insert format
        # ...

        entities = []  # Build entity list
        self._collection.insert(entities)

        return {"took": 0, "errors": False}

    async def search(self, index=None, body=None, **kwargs):
        await self._ensure_connection()

        # Convert OpenSearch DSL to Milvus search
        search_params = self._convert_to_milvus_search(body)
        results = self._collection.search(**search_params)

        # Convert Milvus results to OpenSearch format
        return self._convert_milvus_response(results)

    def _convert_to_milvus_search(self, body):
        """Convert OpenSearch query DSL to Milvus search params."""
        # Implementation details...
        pass

    def _convert_milvus_response(self, results):
        """Convert Milvus response to OpenSearch format."""
        # Implementation details...
        pass
```

**Step 3: Implement Core Operations**

Required methods (OpenSearch-compatible interface):
- `bulk(body, index=None, **kwargs)`
- `search(index=None, body=None, **kwargs)`
- `indices.create(index, body=None, **kwargs)`
- `indices.delete(index, **kwargs)`
- `indices.refresh(index=None, **kwargs)`
- `cluster.health(**kwargs)`
- `info()`

**Step 4: Add Integration Module** (`milvus_integration.py`)

```python
def is_milvus_backend(client_options):
    return "backend" in client_options and client_options["backend"] == "milvus"

def create_milvus_client_factory(hosts, client_options):
    from osbenchmark.milvus_client import MilvusClientFactory
    return MilvusClientFactory(hosts, client_options)
```

#### Phase 2: Runners Implementation

**Step 5: Implement Core Runners** (`milvus_runners.py`)

```python
from osbenchmark.runner import Runner, Delegator

class MilvusBulkIndex(Runner, Delegator):
    async def __call__(self, milvus_client, params):
        request_context_holder = milvus_client
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()  # Fallback timing

        try:
            body = params.get("body")
            await milvus_client.bulk(body=body)

            # Count documents
            doc_count = body.count('\n') // 2

            return {
                "weight": doc_count,
                "unit": "docs",
                "success": True
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

class MilvusVectorSearch(Runner, Delegator):
    async def __call__(self, milvus_client, params):
        request_context_holder = milvus_client
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()

        try:
            index = params.get("index")
            body = params.get("body")

            result = await milvus_client.search(index=index, body=body)

            return {
                "weight": 1,
                "unit": "ops",
                "success": True,
                "hits": result["hits"]["total"]["value"]
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

# Create registry
MILVUS_RUNNERS = {
    "bulk": MilvusBulkIndex(),
    "search": MilvusQuery(),
    "vector-search": MilvusVectorSearch(),
    "create-index": MilvusCreateIndex(),
    "delete-index": MilvusDeleteIndex(),
    "cluster-health": MilvusClusterHealth(),
    # ... add more as needed
}
```

**Important:** Every runner must follow the timing pattern:
1. `on_client_request_start()` at the beginning
2. `on_request_start()` for fallback timing
3. Try/finally block around operation
4. `on_request_end()` and `on_client_request_end()` in finally

#### Phase 3: Integration

**Step 6: Add Integration Points** (`worker_coordinator.py`)

Add Milvus detection to the three integration points:

**Location 1: Backend Detection at Startup** (around line 649)

```python
def receiveMsg_PrepareBenchmark(self, msg, sender):
    client_options = msg.config.opts("client", "options").default

    from osbenchmark import milvus_integration, vespa_integration

    if milvus_integration.is_milvus_backend(client_options):
        from osbenchmark.milvus_client import MilvusClientFactory
        client_factory = MilvusClientFactory
    elif vespa_integration.is_vespa_backend(client_options):
        from osbenchmark.vespa_client import VespaClientFactory
        client_factory = VespaClientFactory
    else:
        from osbenchmark.client import OsClientFactory
        client_factory = OsClientFactory
```

**Location 2: Runner Override System** (around line 1720)

```python
def receiveMsg_WorkloadLoaded(self, msg, sender):
    from osbenchmark import milvus_integration, vespa_integration

    if milvus_integration.is_milvus_backend(client_options):
        from osbenchmark.milvus_runners import MILVUS_RUNNERS
        for op_type, milvus_runner in MILVUS_RUNNERS.items():
            runner.register_runner(op_type, milvus_runner, async_runner=True)
    elif vespa_integration.is_vespa_backend(client_options):
        from osbenchmark.vespa_runners import VESPA_RUNNERS
        for op_type, vespa_runner in VESPA_RUNNERS.items():
            runner.register_runner(op_type, vespa_runner, async_runner=True)
```

**Location 3: Client Creation** (around line 2228)

```python
def _create_clients(self, cluster_settings):
    from osbenchmark import milvus_integration, vespa_integration

    for cluster_name, cluster_options in cluster_settings.items():
        if milvus_integration.is_milvus_backend(cluster_options):
            rest_client_factory = milvus_integration.create_milvus_client_factory(
                cluster_hosts, cluster_options
            )
            opensearch[cluster_name] = rest_client_factory.create_async()
        elif vespa_integration.is_vespa_backend(cluster_options):
            # ... Vespa creation
        else:
            # ... OpenSearch creation
```

## 4. Generalization Recommendations

### 4.1 How OSB Could Be Improved for Multi-Backend Support

The Vespa integration was pragmatic, focused on getting results rather than generalizing OSB. However, several improvements would make future backend integrations easier:

#### 1. Backend Plugin Architecture

**Current State:** Hard-coded backend detection in `worker_coordinator.py`.

**Proposed:** Plugin registry system

```python
# osbenchmark/backends/__init__.py
class BackendPlugin:
    """Abstract base class for backend plugins."""

    @abstractmethod
    def name(self) -> str:
        """Backend name (e.g., 'vespa', 'milvus')."""
        pass

    @abstractmethod
    def detect(self, client_options: dict) -> bool:
        """Return True if this backend should be used."""
        pass

    @abstractmethod
    def create_client_factory(self, hosts, client_options):
        """Create client factory for this backend."""
        pass

    @abstractmethod
    def get_runners(self) -> dict:
        """Return runner registry for this backend."""
        pass

# Auto-discover plugins
REGISTERED_BACKENDS = discover_plugins("osbenchmark.backends")

# In worker_coordinator.py (simplified):
for backend_plugin in REGISTERED_BACKENDS:
    if backend_plugin.detect(client_options):
        factory = backend_plugin.create_client_factory(hosts, client_options)
        break
```

**Benefits:**
- No modifications to `worker_coordinator.py` for new backends
- Backends can be distributed as separate packages
- Clear contract for backend implementations

#### 2. Abstract Base Class for Clients

**Current State:** Each backend implements OpenSearch-compatible interface independently.

**Proposed:** Formal protocol/ABC

```python
from typing import Protocol

class BenchmarkClient(Protocol):
    """Protocol defining required client methods."""

    async def bulk(self, body: str, index: str = None, **kwargs) -> dict:
        """Bulk document ingestion."""
        ...

    async def search(self, index: str = None, body: dict = None, **kwargs) -> dict:
        """Search operation."""
        ...

    async def cluster_health(self, **kwargs) -> dict:
        """Cluster health check."""
        ...

    # ... other required methods

class BackendAsyncClient(RequestContextHolder, BenchmarkClient):
    """Base class for backend clients with timing support."""
    pass
```

**Benefits:**
- Type checking ensures API compatibility
- Documentation is clear about required methods
- IDEs provide better autocomplete

#### 3. Query Translation Framework

**Current State:** Each backend implements query translation independently.

**Proposed:** Pluggable query translator system

```python
class QueryTranslator(ABC):
    """Abstract base for query translators."""

    @abstractmethod
    def translate_knn(self, knn_query: dict) -> Any:
        """Translate KNN query to backend format."""
        pass

    @abstractmethod
    def translate_match(self, match_query: dict) -> Any:
        """Translate match query to backend format."""
        pass

    # ... other query types

class VespaQueryTranslator(QueryTranslator):
    def translate_knn(self, knn_query):
        # Vespa YQL implementation
        return yql_string

class MilvusQueryTranslator(QueryTranslator):
    def translate_knn(self, knn_query):
        # Milvus search params implementation
        return search_params
```

**Benefits:**
- Query translation logic isolated and testable
- Easy to add support for new query types
- Clear documentation of supported queries per backend

#### 4. Standard Timing Hooks

**Current State:** Timing works via mixin inheritance (actually pretty good!).

**Proposed:** Decorator-based timing (alternative approach)

```python
def with_timing(func):
    """Decorator to automatically add timing to operations."""
    async def wrapper(self, *args, **kwargs):
        self.on_client_request_start()
        self.on_request_start()
        try:
            result = await func(self, *args, **kwargs)
            return result
        finally:
            self.on_request_end()
            self.on_client_request_end()
    return wrapper

class BackendAsyncClient(RequestContextHolder):
    @with_timing
    async def bulk(self, body, **kwargs):
        # Implementation without timing boilerplate
        ...

    @with_timing
    async def search(self, body, **kwargs):
        # Implementation without timing boilerplate
        ...
```

**Benefits:**
- Less boilerplate in client methods
- Timing logic centralized in decorator
- Easier to ensure consistency

**Trade-off:** Current mixin approach works well - this is optional improvement.

#### 5. Response Format Adapters

**Current State:** Each backend implements response conversion independently.

**Proposed:** Adapter registry system

```python
class ResponseAdapter(ABC):
    """Abstract base for response format adapters."""

    @abstractmethod
    def adapt_search_response(self, backend_response: Any) -> dict:
        """Convert backend search response to OpenSearch format."""
        pass

    @abstractmethod
    def adapt_bulk_response(self, backend_response: Any) -> dict:
        """Convert backend bulk response to OpenSearch format."""
        pass

# Usage in client:
class BackendAsyncClient:
    def __init__(self, ..., response_adapter: ResponseAdapter):
        self.response_adapter = response_adapter

    async def search(self, ...):
        backend_response = await self._backend_search(...)
        return self.response_adapter.adapt_search_response(backend_response)
```

**Benefits:**
- Response conversion isolated and testable
- Easy to adjust format without touching client logic
- Clear contract for what OpenSearch format looks like

#### 6. Workload Compatibility Matrix

**Current State:** Unclear which workloads work with which backends.

**Proposed:** Declarative compatibility metadata

```python
# In backend plugin:
class VespaBackendPlugin(BackendPlugin):
    def compatibility_matrix(self) -> dict:
        return {
            "vectorsearch": {
                "supported": True,
                "operations": {
                    "bulk": "full",
                    "vector-search": "full",
                    "create-index": "no-op",
                    "force-merge": "no-op"
                },
                "limitations": [
                    "Document-by-document feed (slower ingestion)",
                    "No runtime schema changes"
                ]
            },
            "nyc_taxis": {
                "supported": False,
                "reason": "Requires aggregations not yet implemented"
            }
        }

# CLI can check compatibility:
$ opensearch-benchmark list-workloads --backend=vespa
Vectorsearch: ✓ Supported (with limitations)
  - Document-by-document feed (slower ingestion)
  - No runtime schema changes
NYC Taxis: ✗ Not supported (requires aggregations)
```

**Benefits:**
- Users know what to expect before running workload
- Clear documentation of backend limitations
- Easier to track implementation progress
