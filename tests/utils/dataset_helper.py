# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
import os
import random
import string
from abc import ABC, abstractmethod

import h5py
import numpy as np

from osbenchmark.utils.dataset import Context, BigANNVectorDataSet, HDF5DataSet, BigANNGroundTruthDataSet

DEFAULT_RANDOM_STRING_LENGTH = 8


class DataSetBuildContext:
    """ Data class capturing information needed to build a particular data set

    Attributes:
        data_set_context: Indicator of what the data set is used for,
        vectors: A 2D array containing vectors that are used to build data set.
        path: string representing path where data set should be serialized to.
    """

    def __init__(self, data_set_context: Context, vectors: np.ndarray, path: str):
        self.data_set_context: Context = data_set_context
        self.vectors: np.ndarray = vectors  # TODO: Validate shape
        self.path: str = path

    def get_num_rows(self) -> int:
        return self.vectors.shape[0]

    def get_row_length(self) -> int:
        return self.vectors.shape[1]

    def get_type(self) -> np.dtype:
        return self.vectors.dtype


class DataSetBuilder(ABC):
    """ Abstract builder used to create a build a collection of data sets

    Attributes:
        data_set_build_contexts: list of data set build contexts that builder
                                 will build.
    """

    def __init__(self):
        self.data_set_build_contexts = list()

    def add_data_set_build_context(self, data_set_build_context: DataSetBuildContext):
        """ Adds a data set build context to list of contexts to be built.

        Args:
            data_set_build_context: DataSetBuildContext to be added to list

        Returns: Updated DataSetBuilder

        """
        self._validate_data_set_context(data_set_build_context)
        self.data_set_build_contexts.append(data_set_build_context)
        return self

    def build(self):
        """ Builds and serializes all data sets build contexts
        """
        for data_set_build_context in self.data_set_build_contexts:
            self._build_data_set(data_set_build_context)

    @abstractmethod
    def _build_data_set(self, context: DataSetBuildContext):
        """ Builds an individual data set

        Args:
            context: DataSetBuildContext of data set to be built
        """

    @abstractmethod
    def _validate_data_set_context(self, context: DataSetBuildContext):
        """ Validates that data set context can be added to this builder

        Args:
            context: DataSetBuildContext to be validated
        """


class HDF5Builder(DataSetBuilder):

    def __init__(self):
        super().__init__()
        self.data_set_meta_data = dict()

    def _validate_data_set_context(self, context: DataSetBuildContext):
        if context.path not in self.data_set_meta_data.keys():
            self.data_set_meta_data[context.path] = {
                context.data_set_context: context
            }
            return

        if context.data_set_context in \
                self.data_set_meta_data[context.path].keys():
            raise IllegalDataSetBuildContext("Path and context for data set "
                                             "are already present in builder.")

        self.data_set_meta_data[context.path][context.data_set_context] = \
            context

    @staticmethod
    def _validate_extension(context: DataSetBuildContext):
        ext = context.path.split('.')[-1]

        if ext != HDF5DataSet.FORMAT_NAME:
            raise IllegalDataSetBuildContext("Invalid file extension")

    def _build_data_set(self, context: DataSetBuildContext):
        # For HDF5, because multiple data sets can be grouped in the same file,
        # we will build data sets in memory and not write to disk until
        # _flush_data_sets_to_disk is called
        with h5py.File(context.path, 'a') as hf:
            hf.create_dataset(
                HDF5DataSet.parse_context(context.data_set_context),
                data=context.vectors
            )


class BigANNVectorBuilder(DataSetBuilder):

    def _validate_data_set_context(self, context: DataSetBuildContext):
        self._validate_extension(context)

        # prevent the duplication of paths for data sets
        data_set_paths = [c.path for c in self.data_set_build_contexts]
        if any(data_set_paths.count(x) > 1 for x in data_set_paths):
            raise IllegalDataSetBuildContext("Build context paths have to be "
                                             "unique.")

    @staticmethod
    def _validate_extension(context: DataSetBuildContext):
        ext = context.path.split('.')[-1]

        if ext not in [BigANNVectorDataSet.U8BIN_EXTENSION, BigANNVectorDataSet.FBIN_EXTENSION]:
            raise IllegalDataSetBuildContext("Invalid file extension: {}".format(ext))

        if ext == BigANNVectorDataSet.U8BIN_EXTENSION and context.get_type() != \
                np.uint8:
            raise IllegalDataSetBuildContext("Invalid data type for {} ext."
                                             .format(BigANNVectorDataSet
                                                     .U8BIN_EXTENSION))

        if ext == BigANNVectorDataSet.FBIN_EXTENSION and context.get_type() != \
                np.float32:
            raise IllegalDataSetBuildContext("Invalid data type for {} ext."
                                             .format(BigANNVectorDataSet
                                                     .FBIN_EXTENSION))

    def _build_data_set(self, context: DataSetBuildContext):
        num_vectors = context.get_num_rows()
        dimension = context.get_row_length()
        with open(context.path, 'wb') as f:
            f.write(int.to_bytes(num_vectors, 4, "little"))
            f.write(int.to_bytes(dimension, 4, "little"))
            context.vectors.tofile(f)


class BigANNGroundTruthBuilder(BigANNVectorBuilder):

    @staticmethod
    def _validate_extension(context: DataSetBuildContext):
        ext = context.path.split('.')[-1]

        if ext not in [BigANNGroundTruthDataSet.BIN_EXTENSION]:
            raise IllegalDataSetBuildContext("Invalid file extension: {}".format(ext))

        if context.get_type() != np.float32:
            raise IllegalDataSetBuildContext("Invalid data type for {} ext."
                                             .format(BigANNGroundTruthDataSet
                                                     .BIN_EXTENSION))

    def _build_data_set(self, context: DataSetBuildContext):
        num_queries = context.get_num_rows()
        k = context.get_row_length()
        with open(context.path, 'wb') as f:
            # Writing number of queries
            f.write(int.to_bytes(num_queries, 4, "little"))
            # Writing number of neighbors in a query
            f.write(int.to_bytes(k, 4, "little"))
            # Writing ids of neighbors
            context.vectors.tofile(f)
            # Writing distance of neighbors. For simplicity, we are rewriting the ids to fill the
            # file with distance.
            context.vectors.tofile(f)


def create_attributes(num_vectors: int) -> np.ndarray:
    rng = np.random.default_rng()

    # Random strings and None
    strings = ["str1", "str2", "str3"]

    # First column: random choice from strings
    col1 = rng.choice(strings, num_vectors).astype("S10")

    # Second column: random choice from strings
    col2 = rng.choice(strings, num_vectors).astype("S10")

    # Third column: random numbers between 0 and 100
    col3 = rng.integers(0, 101, num_vectors).astype("S10")

    # Combine columns into a single array
    random_vector = np.column_stack((col1, col2, col3))

    return random_vector

def create_parent_ids(num_vectors: int, group_size: int = 10) -> np.ndarray:
    num_ids = (num_vectors + group_size - 1) // group_size  # Calculate total number of different IDs needed
    ids = np.arange(1, num_ids + 1)  # Create an array of IDs starting from 1
    parent_ids = np.repeat(ids, group_size)[:num_vectors]  # Repeat each ID 'group_size' times and trim to 'num_vectors'
    return parent_ids


def create_random_2d_array(num_vectors: int, dimension: int) -> np.ndarray:
    rng = np.random.default_rng()
    return rng.random(size=(num_vectors, dimension), dtype=np.float32)


class IllegalDataSetBuildContext(Exception):
    """Exception raised when passed in DataSetBuildContext is illegal

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str):
        self.message = f'{message}'
        super().__init__(self.message)


def create_data_set(
        num_vectors: int,
        dimension: int,
        extension: str,
        data_set_context: Context,
        data_set_dir,
        file_path: str = None
) -> str:
    if file_path:
        data_set_path = file_path
    else:
        file_name_base = ''.join(random.choice(string.ascii_letters) for _ in
                                 range(DEFAULT_RANDOM_STRING_LENGTH))
        data_set_file_name = "{}.{}".format(file_name_base, extension)
        data_set_path = os.path.join(data_set_dir, data_set_file_name)
    context = DataSetBuildContext(
        data_set_context,
        create_random_2d_array(num_vectors, dimension),
        data_set_path)

    if extension == HDF5DataSet.FORMAT_NAME:
        HDF5Builder().add_data_set_build_context(context).build()
    else:
        BigANNVectorBuilder().add_data_set_build_context(context).build()

    return data_set_path


def create_attributes_data_set(
        num_vectors: int,
        dimension: int,
        extension: str,
        data_set_context: Context,
        data_set_dir,
        file_path: str = None
) -> str:
    if file_path:
        data_set_path = file_path
    else:
        file_name_base = ''.join(random.choice(string.ascii_letters) for _ in
                                 range(DEFAULT_RANDOM_STRING_LENGTH))
        data_set_file_name = "{}.{}".format(file_name_base, extension)
        data_set_path = os.path.join(data_set_dir, data_set_file_name)
    context = DataSetBuildContext(
        data_set_context,
        create_attributes(num_vectors),
        data_set_path)

    if extension == HDF5DataSet.FORMAT_NAME:
        HDF5Builder().add_data_set_build_context(context).build()
    else:
        BigANNVectorBuilder().add_data_set_build_context(context).build()

    return data_set_path


def create_parent_data_set(
        num_vectors: int,
        dimension: int,
        extension: str,
        data_set_context: Context,
        data_set_dir,
        file_path: str = None
) -> str:
    if file_path:
        data_set_path = file_path
    else:
        file_name_base = ''.join(random.choice(string.ascii_letters) for _ in
                                 range(DEFAULT_RANDOM_STRING_LENGTH))
        data_set_file_name = "{}.{}".format(file_name_base, extension)
        data_set_path = os.path.join(data_set_dir, data_set_file_name)
    context = DataSetBuildContext(
        data_set_context,
        create_parent_ids(num_vectors),
        data_set_path)

    if extension == HDF5DataSet.FORMAT_NAME:
        HDF5Builder().add_data_set_build_context(context).build()
    else:
        BigANNVectorBuilder().add_data_set_build_context(context).build()

    return data_set_path



def create_ground_truth(
        num_queries: int,
        k: int,
        extension: str,
        data_set_context: Context,
        data_set_dir,
        file_path: str = None
) -> str:
    if file_path:
        data_set_path = file_path
    else:
        file_name_base = ''.join(random.choice(string.ascii_letters) for _ in
                                 range(DEFAULT_RANDOM_STRING_LENGTH))
        data_set_file_name = "{}.{}".format(file_name_base, extension)
        data_set_path = os.path.join(data_set_dir, data_set_file_name)
    context = DataSetBuildContext(
        data_set_context,
        create_random_2d_array(num_queries, k),
        data_set_path)

    BigANNGroundTruthBuilder().add_data_set_build_context(context).build()
    return data_set_path
