# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
import tempfile
from unittest import TestCase

from osbenchmark.utils.dataset import Context, get_data_set, HDF5DataSet, BigANNVectorDataSet
from osbenchmark.utils.parse import ConfigurationError
from tests.utils.dataset_helper import create_data_set, create_ground_truth

DEFAULT_INDEX_NAME = "test-index"
DEFAULT_FIELD_NAME = "test-field"
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

    def testBigANNGroundTruthAsAcceptableDataSetFormat(self):
        bin_extension = "bin"
        data_set_dir = tempfile.mkdtemp()

        valid_data_set_path = create_ground_truth(
            100,
            10,
            bin_extension,
            Context.NEIGHBORS,
            data_set_dir
        )
        data_set_instance = get_data_set("bigann", valid_data_set_path, Context.NEIGHBORS)
        self.assertEqual(data_set_instance.FORMAT_NAME, BigANNVectorDataSet.FORMAT_NAME)
        self.assertEqual(data_set_instance.size(), 100)

    def testUnSupportedDataSetFormat(self):
        with self.assertRaises(ConfigurationError) as _:
            get_data_set("random", "/some/path", Context.INDEX)
