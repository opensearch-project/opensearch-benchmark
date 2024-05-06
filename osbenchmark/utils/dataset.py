# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import os
import struct
from abc import ABC, ABCMeta, abstractmethod
from enum import Enum
from typing import cast

import h5py
import numpy as np

from osbenchmark.exceptions import InvalidExtensionException
from osbenchmark.utils.parse import ConfigurationError


class Context(Enum):
    """DataSet context enum. Can be used to add additional context for how a
    data-set should be interpreted.
    """
    INDEX = 1
    QUERY = 2
    NEIGHBORS = 3


class DataSet(ABC):
    """DataSet interface. Used for reading data-sets from files.

    Methods:
        read: Read a chunk of data from the data-set
        seek: Get to position in the data-set
        size: Gets the number of items in the data-set
        reset: Resets internal state of data-set to beginning
    """
    __metaclass__ = ABCMeta

    BEGINNING = 0

    @abstractmethod
    def read(self, chunk_size: int):
        """Read vector for given chunk size
        @param chunk_size: limits vector size to read
        """

    @abstractmethod
    def seek(self, offset: int):
        """
        Move reader to given offset
        @param offset: value to move reader pointer to
        """

    @abstractmethod
    def size(self):
        """
        Returns size of dataset
        """

    @abstractmethod
    def reset(self):
        """
        Resets the dataset reader
        """


def get_data_set(data_set_format: str, path: str, context: Context):
    """
    Factory method to get instance of Dataset for given format.
    Args:
        data_set_format: File format like hdf5, bigann
        path: Data set file path
        context: Dataset Context Enum
    Returns: DataSet instance
    """
    if data_set_format == HDF5DataSet.FORMAT_NAME:
        return HDF5DataSet(path, context)
    if data_set_format == BigANNVectorDataSet.FORMAT_NAME:
        return create_big_ann_dataset(path)
    raise ConfigurationError("Invalid data set format")


class HDF5DataSet(DataSet):
    """ Data-set format corresponding to `ANN Benchmarks
    <https://github.com/erikbern/ann-benchmarks#data-sets>`_
    """

    FORMAT_NAME = "hdf5"

    def __init__(self, dataset_path: str, context: Context):
        self.dataset_path = dataset_path
        self.context = self.parse_context(context)
        self.current = self.BEGINNING
        self.data = None

    def _load(self):
        if self.data is None:
            file = h5py.File(self.dataset_path)
            self.data = cast(h5py.Dataset, file[self.context])

    def read(self, chunk_size: int):
        # load file first before read
        self._load()
        if self.current >= self.size():
            return None

        end_offset = self.current + chunk_size
        if end_offset > self.size():
            end_offset = self.size()

        vectors = cast(np.ndarray, self.data[self.current:end_offset])
        self.current = end_offset
        return vectors

    def seek(self, offset: int):
        if offset < self.BEGINNING:
            raise Exception("Offset must be greater than or equal to 0")

        if offset >= self.size():
            raise Exception("Offset must be less than the data set size")

        self.current = offset

    def size(self):
        # load file first before return size
        self._load()
        return self.data.len()

    def reset(self):
        self.current = self.BEGINNING

    @staticmethod
    def parse_context(context: Context) -> str:
        if context == Context.NEIGHBORS:
            return "neighbors"

        if context == Context.INDEX:
            return "train"

        if context == Context.QUERY:
            return "test"

        raise Exception("Unsupported context")


class BigANNDataSet(DataSet):

    DATA_SET_HEADER_LENGTH = 8
    FORMAT_NAME = "bigann"

    def __init__(self, dataset_path: str):
        self.dataset_path = dataset_path
        self.file = None
        self.num_bytes = 0
        self.current = DataSet.BEGINNING
        self.bytes_per_num = 0
        self.rows = 0
        self.row_length = 0

    def _init_internal_params(self):
        self.file = open(self.dataset_path, 'rb')
        self.file.seek(DataSet.BEGINNING, os.SEEK_END)
        self.num_bytes = self.file.tell()
        if self.num_bytes < BigANNDataSet.DATA_SET_HEADER_LENGTH:
            raise Exception("Invalid file: file size cannot be less than {} bytes".format(
                BigANNDataSet.DATA_SET_HEADER_LENGTH))
        self.file.seek(BigANNDataSet.BEGINNING)
        self.rows = int.from_bytes(self.file.read(4), "little")
        self.row_length = int.from_bytes(self.file.read(4), "little")
        self.bytes_per_num = self._get_data_size()
        self.reader = self._value_reader()

    def _load(self):
        # load file if it is not loaded yet
        if self.file is None:
            self._init_internal_params()

    def read(self, chunk_size: int):
        # load file first before read
        self._load()
        if self.current >= self.size():
            return None

        end_offset = self.current + chunk_size
        if end_offset > self.size():
            end_offset = self.size()

        vectors = np.asarray(
            [self._read_vector() for _ in range(end_offset - self.current)]
        )
        self.current = end_offset
        return vectors

    def _get_file_byte_offset(self, offset):
        """Return file byte offset for given offset"""
        return BigANNDataSet.DATA_SET_HEADER_LENGTH + (self.row_length * self.bytes_per_num * offset)

    def seek(self, offset: int):
        # load file first before seek
        self._load()
        if offset < self.BEGINNING:
            raise Exception("Offset must be greater than or equal to 0")

        if offset >= self.size():
            raise Exception("Offset must be less than the data set size")

        bytes_offset = self._get_file_byte_offset(offset)
        self.file.seek(bytes_offset)
        self.current = offset

    def _read_vector(self):
        return np.asarray([self.reader(self.file) for _ in range(self.row_length)])

    def size(self):
        # load file first before return size
        self._load()
        return self.rows

    def reset(self):
        if self.file:
            self.file.seek(BigANNDataSet.DATA_SET_HEADER_LENGTH)
        self.current = BigANNDataSet.BEGINNING

    def __del__(self):
        if self.file:
            self.file.close()

    @abstractmethod
    def _get_supported_extension(self):
        """Return list of supported extension by this dataset"""

    def _get_extension(self):
        ext = self.dataset_path.split('.')[-1]
        supported_extension = self._get_supported_extension()
        if ext not in supported_extension:
            raise InvalidExtensionException(
                "Unknown extension :{}, supported extensions are: {}".format(
                    ext, str(supported_extension)))
        return ext

    @abstractmethod
    def get_data_size(self, extension):
        """Return data size based on extension"""

    def _get_data_size(self):
        """Return data size"""
        ext = self._get_extension()
        return self.get_data_size(ext)

    @abstractmethod
    def _get_value_reader(self, extension):
        """Return value reader based on extension"""

    def _value_reader(self):
        ext = self._get_extension()
        return self._get_value_reader(ext)


class BigANNVectorDataSet(BigANNDataSet):
    """ Data-set format for vector data-sets for `Big ANN Benchmarks
    <https://big-ann-benchmarks.com/index.html#bench-datasets>`
    """

    U8BIN_EXTENSION = "u8bin"
    FBIN_EXTENSION = "fbin"
    SUPPORTED_EXTENSION = [
        FBIN_EXTENSION, U8BIN_EXTENSION
    ]

    BYTES_PER_U8INT = 1
    BYTES_PER_FLOAT = 4

    def _init_internal_params(self):
        super()._init_internal_params()
        if (self.num_bytes - BigANNDataSet.DATA_SET_HEADER_LENGTH) != (
                self.rows * self.row_length * self.bytes_per_num):
            raise Exception("Invalid file. File size is not matching with expected estimated "
                            "value based on number of points, dimension and bytes per point")

    def _get_supported_extension(self):
        return BigANNVectorDataSet.SUPPORTED_EXTENSION

    def get_data_size(self, extension):
        if extension == BigANNVectorDataSet.U8BIN_EXTENSION:
            return BigANNVectorDataSet.BYTES_PER_U8INT

        if extension == BigANNVectorDataSet.FBIN_EXTENSION:
            return BigANNVectorDataSet.BYTES_PER_FLOAT

        return None

    def _get_value_reader(self, extension):
        if extension == BigANNVectorDataSet.U8BIN_EXTENSION:
            return lambda file: float(int.from_bytes(file.read(BigANNVectorDataSet.BYTES_PER_U8INT), "little"))

        if extension == BigANNVectorDataSet.FBIN_EXTENSION:
            return lambda file: struct.unpack('<f', file.read(BigANNVectorDataSet.BYTES_PER_FLOAT))

        return None


class BigANNGroundTruthDataSet(BigANNDataSet):
    """ Data-set format for neighbor data-sets for `Big ANN Benchmarks
    <https://big-ann-benchmarks.com/index.html#bench-datasets>`"""

    BIN_EXTENSION = "bin"
    SUPPORTED_EXTENSION = [BIN_EXTENSION]

    BYTES_PER_UNSIGNED_INT32 = 4

    def _init_internal_params(self):
        super()._init_internal_params()
        # The ground truth binary files consist of the following information:
        # num_queries(uint32_t) K-NN(uint32) followed by num_queries X K x sizeof(uint32_t) bytes of data
        # representing the IDs of the K-nearest neighbors of the queries, followed by num_queries X K x sizeof(float)
        # bytes of data representing the distances to the corresponding points.
        if (self.num_bytes - BigANNDataSet.DATA_SET_HEADER_LENGTH) != 2 * (
                self.rows * self.row_length * self.bytes_per_num):
            raise Exception("Invalid file. File size is not matching with expected estimated "
                            "value based on number of queries, k and bytes per query")

    def _get_supported_extension(self):
        return BigANNGroundTruthDataSet.SUPPORTED_EXTENSION

    def get_data_size(self, extension):
        return BigANNGroundTruthDataSet.BYTES_PER_UNSIGNED_INT32

    def _get_value_reader(self, extension):
        return lambda file: int.from_bytes(
            file.read(BigANNGroundTruthDataSet.BYTES_PER_UNSIGNED_INT32), "little")


def create_big_ann_dataset(file_path: str):
    if not file_path:
        raise Exception("Invalid file path")
    extension = file_path.split('.')[-1]
    if extension in BigANNGroundTruthDataSet.SUPPORTED_EXTENSION:
        return BigANNGroundTruthDataSet(file_path)
    if extension in BigANNVectorDataSet.SUPPORTED_EXTENSION:
        return BigANNVectorDataSet(file_path)
    raise Exception("Unsupported file")
