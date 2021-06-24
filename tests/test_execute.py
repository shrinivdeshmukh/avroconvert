#!/usr/bin/env python

"""Tests for `avroconvert` package."""


from unittest import mock, TestCase
import avroconvert as avc
import concurrent


class Execute(TestCase):

    @classmethod
    def setUpClass(self):
        pass

    def test_resolve(self):
        bytes_data = {'filename1': b'test data1', 'filename2': 'test data2'}
        avc.gs_reader = mock.Mock(name='gs_reader', return_value=True)
        exec_obj = avc.Execute(source='gs', bucket='test-bucket', dst_format='parquet', outfolder='./test-output-folder')
        run_res = exec_obj._resolve()
        self.assertEqual(True, run_res)

    @mock.patch('avroconvert.execute.concurrent')
    @mock.patch('avroconvert.execute.cpu_count')
    def test_run(self, mock_cpu_count, mock_concurrent):
        bytes_data = {'filename1': b'test data1', 'filename2': 'test data2'}
        avc.s3_reader = mock.Mock(name='s3_reader')

        mock_cpu_count.return_value = 2 # Set the cpu count to 2
        exec_obj = avc.Execute(source='gs', bucket='test-bucket', dst_format='parquet', outfolder='./test-output-folder')
        exec_obj._resolve = mock.Mock(name='s3_reader')
        exec_obj._resolve().get_data = mock.Mock(return_value=bytes_data)
        function_response = exec_obj.run()
        self.assertEqual(True, function_response)

        mock_concurrent.futures.ProcessPoolExecutor.assert_called_with(max_workers=4) # Formula used to calculate total process is cpu_count * 2

    def test_run_no_data(self):
        avc.s3_reader = mock.Mock(name='s3_reader')

        exec_obj = avc.Execute(source='gs', bucket='test-bucket', dst_format='parquet', outfolder='./test-output-folder')
        exec_obj._resolve = mock.Mock(name='s3_reader')
        exec_obj._resolve().get_data = mock.Mock(return_value={})
        function_response = exec_obj.run()
        self.assertEqual(None, function_response)
    
    def test_execute_missing_output_format(self):
        with self.assertRaises(AttributeError) as e:
            exec_obj = avc.Execute(source='abc', bucket='test-bucket', dst_format=None, outfolder='test-output', prefix='test-prefix')
        self.assertEqual(("Output format not specified, should be one of ['parquet', 'csv', 'json']",), e.exception.args)

    def test_execute_validate_output_format(self):
        with self.assertRaises(Exception) as e:
            exec_obj = avc.Execute(source='gs', bucket='test-bucket', dst_format='random_format', outfolder='test-output', prefix='test-prefix')
        self.assertEqual(("Invalid format random_format. It should be one of ['parquet', 'csv', 'json']",), e.exception.args)

    def test_execute_validate_source(self):
        with self.assertRaises(Exception) as e:
            exec_obj = avc.Execute(source='random_source', bucket='test-bucket', dst_format='parquet', outfolder='test-output', prefix='test-prefix')
        self.assertEqual(("Invalid source random_source passed. Source should be one of ['s3', 'gs', 'fs']",), e.exception.args)

    def test_execute_validate_bucket(self):
        with self.assertRaises(Exception) as e:
            exec_obj = avc.Execute(source='s3', bucket=None, dst_format='parquet', outfolder='test-output', prefix='test-prefix')
        self.assertEqual(("Please specify a bucket",), e.exception.args)

    def test_execute_validate_outfolder(self):
        with self.assertRaises(Exception) as e:
            exec_obj = avc.Execute(source='s3', bucket=None, dst_format='parquet', outfolder=None, prefix='test-prefix')
        self.assertEqual(("Please specify an output folder",), e.exception.args)

    @classmethod
    def tearDownClass(self):
        pass