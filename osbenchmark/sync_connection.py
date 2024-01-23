# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import opensearchpy


class RequestsHttpConnection(opensearchpy.RequestsHttpConnection):
    def __init__(self,
                host= "localhost",
                port= None,
                http_auth= None,
                use_ssl= False,
                ssl_assert_fingerprint=None,
                pool_maxsize=None,
                headers = None,
                ssl_context = None,
                http_compress = None,
                opaque_id = None,
                **kwargs,):
        super().__init__(host=host,
                         port=port,
                         http_auth=http_auth,
                         use_ssl=use_ssl,
                         ssl_assert_fingerprint=ssl_assert_fingerprint,
                         pool_maxsize=max(256, kwargs.get("max_connections", 0)),
                         headers=headers,
                         ssl_context=ssl_context,
                         http_compress=http_compress,
                         opaque_id=opaque_id,
                         **kwargs,)

    def perform_request(self, method, url, params=None, body=None, timeout=None, allow_redirects=True, ignore=(), headers=None):
        try:
            # pylint: disable=import-outside-toplevel
            from osbenchmark.client import RequestContextHolder
            request_context_holder = RequestContextHolder()
            request_context_holder.on_request_start()
            status, headers, raw_data = super().perform_request(method=method, url=url, params=params, body=body, timeout=timeout,
                                                                allow_redirects=allow_redirects, ignore=ignore, headers=headers)
            request_context_holder.on_request_end()
            return status, headers, raw_data
        except LookupError:
            status, headers, raw_data = super().perform_request(method=method, url=url, params=params, body=body, timeout=timeout,
                                                                allow_redirects=allow_redirects, ignore=ignore, headers=headers)
            return status, headers, raw_data


class Urllib3HttpConnection(opensearchpy.Urllib3HttpConnection):
    def __init__(self,
                host= "localhost",
                port= None,
                http_auth= None,
                use_ssl= False,
                ssl_assert_fingerprint=None,
                pool_maxsize=None,
                headers = None,
                ssl_context = None,
                http_compress = None,
                opaque_id = None,
                **kwargs,):
        super().__init__(host=host,
                         port=port,
                         http_auth=http_auth,
                         use_ssl=use_ssl,
                         ssl_assert_fingerprint=ssl_assert_fingerprint,
                         pool_maxsize=max(256, kwargs.get("max_connections", 0)),
                         headers=headers,
                         ssl_context=ssl_context,
                         http_compress=http_compress,
                         opaque_id=opaque_id,
                         **kwargs,)

    def perform_request(self, method, url, params=None, body=None, timeout=None, ignore=(), headers=None):
        try:
            # pylint: disable=import-outside-toplevel
            from osbenchmark.client import RequestContextHolder
            request_context_holder = RequestContextHolder()
            request_context_holder.on_request_start()
            status, headers, raw_data = super().perform_request(method=method, url=url, params=params, body=body,
                                                                timeout=timeout, ignore=ignore, headers=headers)
            request_context_holder.on_request_end()
            return status, headers, raw_data
        except LookupError:
            status, headers, raw_data = super().perform_request(method=method, url=url, params=params, body=body,
                                                                timeout=timeout, ignore=ignore, headers=headers)
            return status, headers, raw_data
