#!/usr/bin/env python

"""Tests for `avroconvert` package."""


from unittest import mock, TestCase

from avroconvert import AvroConvert as avc, logger

import pandas as pd
from pyarrow import Table
from os.path import dirname


class TestAvroconvert(TestCase):
    """Tests for `avroconvert` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        self.bytes_data = [b'test data1', b'test data2']
        self.str_data = ['test data1', 'test data2']

    @mock.patch('avroconvert.avroconvert.BytesIO')
    @mock.patch('avroconvert.avroconvert.reader')
    def test_convert_avro_w_csv_output(self, mock_rdr, mock_btio):
        logger.info('[CSV] testing test_convert_avro_w_csv_output function')

        avc_obj = avc(outfolder='./test_output', dst_format='csv')

        mock_btio.side_effects = self.bytes_data
        mock_rdr.__iter__.return_value = self.str_data

        avc_obj._change_file_extn = mock.Mock(return_value='testinput.csv')
        avc_obj._check_output_folder = mock.Mock(side_effect=[True, True])
        avc_obj._to_csv = mock.Mock(return_value=True)

        actual_response = avc_obj.convert_avro(
            filename='testinput.avro', data=self.bytes_data)

        avc_obj._change_file_extn.assert_called_with('testinput.avro')
        avc_obj._to_csv.assert_called_with(
            data=[], outfile='./test_output/testinput.csv')
        mock_btio.assert_has_calls([mock.call(self.bytes_data)])
        mock_rdr.assert_has_calls([mock.call(mock_btio())])
        self.assertEqual(
            actual_response, 'File ./test_output/testinput.csv complete')
        print("")

    @mock.patch('avroconvert.avroconvert.BytesIO')
    @mock.patch('avroconvert.avroconvert.reader')
    def test_convert_avro_w_json_output(self, mock_rdr, mock_btio):
        logger.info('[JSON] testing test_convert_avro_w_json_output function')
        avc_obj = avc(outfolder='./test_output', dst_format='json')

        mock_btio.side_effects = self.bytes_data
        mock_rdr.__iter__.return_value = self.str_data

        avc_obj._change_file_extn = mock.Mock(return_value='testinput.json')
        avc_obj._check_output_folder = mock.Mock(side_effect=[True, True])
        avc_obj._to_json = mock.Mock(return_value=True)

        actual_response = avc_obj.convert_avro(
            filename='testinput.avro', data=self.bytes_data)

        avc_obj._change_file_extn.assert_called_with('testinput.avro')
        avc_obj._to_json.assert_called_with(
            data=[], outfile='./test_output/testinput.json')
        mock_btio.assert_has_calls([mock.call(self.bytes_data)])
        mock_rdr.assert_has_calls([mock.call(mock_btio())])
        self.assertEqual(
            actual_response, 'File ./test_output/testinput.json complete')
        print("")

    @mock.patch('avroconvert.avroconvert.BytesIO')
    @mock.patch('avroconvert.avroconvert.reader')
    def test_convert_avro_w_parquet_output(self, mock_rdr, mock_btio):
        logger.info(
            '[PARQUET] testing test_convert_avro_w_parquet_output function')
        avc_obj = avc(outfolder='./test_output', dst_format='parquet')

        mock_btio.side_effects = self.bytes_data
        mock_rdr.__iter__.return_value = self.str_data

        avc_obj._change_file_extn = mock.Mock(return_value='testinput.parquet')
        avc_obj._check_output_folder = mock.Mock(side_effect=[True, True])
        avc_obj._to_parquet = mock.Mock(return_value=True)

        actual_response = avc_obj.convert_avro(
            filename='testinput.avro', data=self.bytes_data)

        avc_obj._change_file_extn.assert_called_with('testinput.avro')
        avc_obj._to_parquet.assert_called_with(
            data=[], outfile='./test_output/testinput.parquet')
        mock_btio.assert_has_calls([mock.call(self.bytes_data)])
        mock_rdr.assert_has_calls([mock.call(mock_btio())])
        self.assertEqual(
            actual_response, 'File ./test_output/testinput.parquet complete')
        print("")

    def test_convert_avro_w_no_data(self):
        logger.info(
            '[PARQUET] testing test_convert_avro_w_parquet_output function')
        avc_obj = avc(outfolder='./test_output', dst_format='parquet')

        actual_response = avc_obj.convert_avro(
            filename='testinput.avro', data=None)

        self.assertEqual(actual_response, None)
        print("")

    @mock.patch('avroconvert.avroconvert.DataFrame')
    @mock.patch('avroconvert.avroconvert.Table')
    @mock.patch('avroconvert.avroconvert.write_table')
    def test_to_parquet(self, mock_write_table, mock_table, mock_df):
        logger.info(
            '[TO_PARQUET] testing test_to_parquet function')
        outfile = './test_output_folder/test.parquet'

        avc_obj = avc(outfolder='./test_output', dst_format='parquet')
        avc_obj._check_output_folder = mock.Mock(side_effect=[True, True])
        data = [{'name': 'John', 'address': 'New York'},
                {'name': 'Jane', 'address': 'Mumbai'}]
        df = pd.DataFrame(data)
        arrow_table = Table.from_pandas(df)

        mock_table.from_pandas.return_value = arrow_table
        mock_df.return_value = df

        actual_response = avc_obj._to_parquet(data=data, outfile=outfile)

        mock_df.assert_called_with(data)
        mock_table.from_pandas.assert_called_with(df)
        mock_write_table.assert_called_with(
            arrow_table, outfile, flavor='spark')

        self.assertEqual(actual_response, outfile)

    @mock.patch('avroconvert.avroconvert.DataFrame')
    def test_to_json(self, mock_df):
        logger.info(
            '[TO_JSON] testing test_to_json function')
        outfile = './test_output_folder/test.json'

        avc_obj = avc(outfolder='./test_output', dst_format='json')
        avc_obj._check_output_folder = mock.Mock(side_effect=[True, True])

        data = [{'name': 'John', 'address': 'New York'},
                {'name': 'Jane', 'address': 'Mumbai'}]
        df = pd.DataFrame(data)

        mock_df.return_value = df

        with mock.patch.object(df, "to_json") as to_json_mock:
            actual_response = avc_obj._to_json(data=data, outfile=outfile)
            to_json_mock.assert_called_with(outfile, orient='records')

        self.assertEqual(actual_response, outfile)
        
        print("")

    @mock.patch('avroconvert.avroconvert.exists')
    @mock.patch('avroconvert.avroconvert.Path')
    def test_check_output_folder_if(self, mock_path, mock_exists):
        logger.info(
            '[CHECK_OUTPUT_FOLDER] testing test_check_output_folder function')
        folderpath = 'folder1'

        mock_exists.side_effect = [True, False]

        avc_obj = avc(outfolder='./folder1', dst_format='json')

        res = avc_obj._check_output_folder(folderpath=folderpath)

        mock_exists.assert_called_with(folderpath)
        print("")

    @mock.patch('avroconvert.avroconvert.exists')
    @mock.patch('avroconvert.avroconvert.Path')
    def test_check_output_folder_else(self, mock_path, mock_exists):
        logger.info(
            '[CHECK_OUTPUT_FOLDER] testing test_check_output_folder function')
        folderpath = 'folder1/folder2'

        mock_exists.side_effect = [True, False]

        avc_obj = avc(outfolder='./folder1/folder2', dst_format='json')

        res = avc_obj._check_output_folder(folderpath=folderpath)

        mock_exists.assert_called_with(dirname(folderpath))
        print("")
        
    @mock.patch('avroconvert.avroconvert.Path')
    def test_change_file_extn(self, mock_path):
        logger.info('[CHANGE_FILE_EXTN]')
        avc_obj = avc(outfolder='./folder1/folder2', dst_format='json')

        mock_path.parent.joinpath.return_value = 'folder1/file1.json'

        mock_path.stem.return_value = './folder1/file1'

        res = avc_obj._change_file_extn('./folder1/file1.avro')

        self.assertEqual(res, f"{mock_path().parent.joinpath()}")

    def tearDown(self):
        """Tear down test fixtures, if any."""
