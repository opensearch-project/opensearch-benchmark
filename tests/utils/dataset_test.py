# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
import tempfile
from unittest import TestCase

from osbenchmark.utils.dataset import Context, get_data_set, HDF5DataSet, BigANNVectorDataSet, ParquetDataSet
from osbenchmark.utils.parse import ConfigurationError
from tests.utils.dataset_helper import create_data_set

DEFAULT_INDEX_NAME = "test-index"
DEFAULT_FIELD_NAME = "test-field"
DEFAULT_DATASET_NAME = "emb"
DEFAULT_CONTEXT = Context.INDEX
DEFAULT_NUM_VECTORS = 10
DEFAULT_DIMENSION = 10
DEFAULT_RANDOM_STRING_LENGTH = 8


class DataSetTestCase(TestCase):

    def testHDF5AsAcceptableDataSetFormat(self):
        with tempfile.TemporaryDirectory() as data_set_dir:
            valid_data_set_path = create_data_set(
                DEFAULT_NUM_VECTORS,
                DEFAULT_DIMENSION,
                HDF5DataSet.FORMAT_NAME,
                DEFAULT_CONTEXT,
                data_set_dir
            )
            data_set_instance = get_data_set("hdf5", valid_data_set_path, Context.INDEX)
            self.assertEqual(data_set_instance.FORMAT_NAME, HDF5DataSet.FORMAT_NAME)
            self.assertEqual(data_set_instance.size(), DEFAULT_NUM_VECTORS)

    def testParquetAsAcceptableDataSetFormat(self):
        with tempfile.TemporaryDirectory() as data_set_dir:
            valid_data_set_path = create_data_set(
                700,
                DEFAULT_DIMENSION,
                ParquetDataSet.FORMAT_NAME,
                DEFAULT_CONTEXT,
                data_set_dir,
                column_name=DEFAULT_DATASET_NAME,
            )
            data_set_instance = get_data_set("parquet", valid_data_set_path, column_name=DEFAULT_DATASET_NAME)
            self.assertEqual(data_set_instance.FORMAT_NAME, ParquetDataSet.FORMAT_NAME)
            self.assertEqual(data_set_instance.size(), 700)
            actual_vectors = []
            while True:
                partial = data_set_instance.read(200)
                if partial is None:
                    self.assertEqual(len(actual_vectors), 700)
                    break
                # last fetch will have 100 records
                self.assertLessEqual(len(partial), 200)
                actual_vectors.extend(partial)
            # Try with reset
            data_set_instance.reset()
            actual_vectors = []
            while True:
                partial = data_set_instance.read(100)
                if partial is None:
                    self.assertEqual(len(actual_vectors), 700)
                    break
                self.assertEqual(len(partial), 100)
                actual_vectors.extend(partial)

    def testBigANNAsAcceptableDataSetFormatWithFloatExtension(self):
        float_extension = "fbin"
        data_set_dir = tempfile.mkdtemp()

        valid_data_set_path = create_data_set(
            DEFAULT_NUM_VECTORS,
            DEFAULT_DIMENSION,
            float_extension,
            DEFAULT_CONTEXT,
            data_set_dir
        )
        data_set_instance = get_data_set("bigann", valid_data_set_path, Context.INDEX)
        self.assertEqual(data_set_instance.FORMAT_NAME, BigANNVectorDataSet.FORMAT_NAME)
        self.assertEqual(data_set_instance.size(), DEFAULT_NUM_VECTORS)

    def testUnSupportedDataSetFormat(self):
        with self.assertRaises(ConfigurationError) as _:
            get_data_set("random", "/some/path", Context.INDEX)
