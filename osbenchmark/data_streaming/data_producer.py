# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from abc import ABC, abstractmethod

class DataProducer(ABC):

    @abstractmethod
    def generate_chunked_data(self) -> None:
        pass
