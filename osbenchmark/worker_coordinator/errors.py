import re

def parse_error(error_metadata):
    error = error_metadata['error']
    status_code = None
    description = "error occured, check logs for details"
    operation = UnknownOperationError(description, None)

    if 'status' in error_metadata:
        status_code = error_metadata["status"]

    if 'reason' in error:
        description = error['reason']
        matches = re.findall(r'\[([^]]*)\]', description)
        for match in matches:
            if match == "indices:admin/create":
                operation = IndexOperationError(description, "index-create", status_code)
            elif match == "indices:admin/delete":
                operation =  IndexOperationError(description, "index-delete", status_code)
            elif match == "indices:data/write/bulk":
                operation = IndexOperationError(description, "index-append", status_code)
            elif match == "indices:admin/refresh":
                operation = IndexOperationError(description, "refresh-after-index", status_code)
            elif match == "indices:admin/forcemerge":
                operation = IndexOperationError(description, "force-merge", status_code)
            elif match == "indices:data/read/search":
                operation =  SearchOperationError(description, "search", status_code)

    return operation


class OpenSearchOperationError():
    def __init__(self, description, operation=None, status_code=None):
        self.description = description
        self.operation = operation
        self.status_code = status_code

class UnknownOperationError(OpenSearchOperationError):
    def get_error_message(self):
        return self.description


class IndexOperationError(OpenSearchOperationError):
    def get_error_message(self):
        if self.status_code == 403:
            return f"permission denied for {self.operation}. check logs for details"
        elif self.status_code == 500:
            return f"internal server error for {self.operation}. check logs for details"
        else:
            return self.description

class SearchOperationError(OpenSearchOperationError):
    def get_error_message(self):
        if self.status_code == 403:
            return f"permission denied for {self.operation}. check logs for details"
        elif self.status_code == 500:
            return f"internal server error for {self.operation} index. check logs for details"
        else:
            return self.description
