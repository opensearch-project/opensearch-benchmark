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
Defines the contract that all database clients must implement.

This interface mirrors the opensearchpy.OpenSearch client structure to ensure
compatibility with OSB runners while allowing different database backends.

The interface uses a pass-through wrapper pattern:
- For OpenSearch: delegates directly to opensearchpy client
- For other databases: translates operations to database-specific APIs
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class IndicesNamespace(ABC):
    """Namespace for index management operations"""

    @abstractmethod
    async def create(self, index: str, body: Optional[Dict] = None, **kwargs) -> Dict:
        """Create an index"""

    @abstractmethod
    async def delete(self, index: str, **kwargs) -> Dict:
        """Delete an index"""

    @abstractmethod
    async def exists(self, index: str, **kwargs) -> bool:
        """Check if index exists"""

    @abstractmethod
    async def refresh(self, index: Optional[str] = None, **kwargs) -> Dict:
        """Refresh one or more indices"""

    @abstractmethod
    def stats(self, index: Optional[str] = None, metric: Optional[str] = None, **kwargs) -> Dict:
        """Get index statistics (sync - called by telemetry)"""

    @abstractmethod
    def forcemerge(self, index: Optional[str] = None, **kwargs) -> Dict:
        """Force merge index segments (sync - called by telemetry)"""


class ClusterNamespace(ABC):
    """Namespace for cluster-level operations"""

    @abstractmethod
    async def health(self, **kwargs) -> Dict:
        """Get cluster health status"""

    @abstractmethod
    async def put_settings(self, body: Dict, **kwargs) -> Dict:
        """Update cluster settings"""


class TransportNamespace(ABC):
    """Low-level transport for custom API endpoints"""

    @abstractmethod
    async def perform_request(self, method: str, url: str,
                             params: Optional[Dict] = None,
                             body: Optional[Any] = None,
                             headers: Optional[Dict] = None) -> Any:
        """Perform a raw HTTP request"""


class NodesNamespace(ABC):
    """Namespace for node-level operations and statistics"""

    @abstractmethod
    def stats(self, node_id: Optional[str] = None,
              metric: Optional[str] = None,
              **kwargs) -> Dict:
        """Get node statistics"""

    @abstractmethod
    def info(self, node_id: Optional[str] = None,
             metric: Optional[str] = None,
             **kwargs) -> Dict:
        """Get node information"""


class DatabaseClient(ABC):
    """
    Abstract interface for database clients.

    All database implementations must provide this interface to be compatible
    with OSB runners. The interface is designed to match opensearchpy.OpenSearch
    structure but can be implemented by any search/database engine.
    """

    # Namespaced APIs (properties that return namespace objects)
    @property
    @abstractmethod
    def indices(self) -> IndicesNamespace:
        """Access to indices namespace"""

    @property
    @abstractmethod
    def cluster(self) -> ClusterNamespace:
        """Access to cluster namespace"""

    @property
    @abstractmethod
    def transport(self) -> TransportNamespace:
        """Access to transport namespace"""

    @property
    @abstractmethod
    def nodes(self) -> NodesNamespace:
        """Access to nodes namespace"""

    # Core document operations
    @abstractmethod
    async def bulk(self, body: Union[str, List],
                   index: Optional[str] = None,
                   doc_type: Optional[str] = None,
                   params: Optional[Dict] = None,
                   **kwargs) -> Dict:
        """Bulk index/update/delete documents"""

    @abstractmethod
    async def index(self, index: str, body: Dict,
                   id: Optional[str] = None,
                   doc_type: Optional[str] = None,
                   **kwargs) -> Dict:
        """Index a single document"""

    @abstractmethod
    async def search(self, index: Optional[str] = None,
                    body: Optional[Dict] = None,
                    doc_type: Optional[str] = None,
                    **kwargs) -> Dict:
        """Execute a search query"""

    def info(self) -> Dict:
        """
        Get cluster/database information.

        Returns version, build info, etc. Similar to OpenSearch's root endpoint.
        """
        return {}

    # Additional methods used by runners
    def return_raw_response(self):
        """
        Configure client to return raw responses (for performance).
        Optional method - implementations can provide no-op.
        """
        pass

    def close(self):
        """
        Close client connections.
        Optional method - implementations can provide no-op.
        """
