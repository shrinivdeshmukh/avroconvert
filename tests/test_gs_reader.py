from unittest import TestCase, mock
from avroconvert.sources.gcs.reader import GCS
from os import environ


class TestGcsReader(TestCase):

    @classmethod
    def setUpClass(cls):
        environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'test-service.json'

    @mock.patch('avroconvert.sources.gcs.reader.storage')
    def test_extract_raw_data(self, mock_strg):
        gcs_client = mock.MagicMock()
        gcs_client.get_bucket = mock.MagicMock()
        mock_strg.Client.from_service_account_json.return_value = gcs_client
        gcs_reader = GCS(bucket='test')
        gcs_reader._filter = mock.Mock(
            return_value=['file1.avro', 'file2.avro'])
        gcs_reader._read_files = mock.Mock(side_effect=[b'data1', b'data2'])
        extract_response = gcs_reader._extract_raw_data()
        gcs_reader._filter.assert_called_with()
        gcs_reader._read_files.assert_has_calls(
            [mock.call(filename='file1.avro'), mock.call(filename='file2.avro')])
        self.assertEqual(
            {'file1.avro': b'data1', 'file2.avro': b'data2'}, extract_response)

    @mock.patch('avroconvert.sources.gcs.reader.storage')
    def test_extract_raw_data_w_no_files(self, mock_strg):
        gcs_client = mock.MagicMock()
        gcs_client.get_bucket = mock.MagicMock()
        mock_strg.Client.from_service_account_json.return_value = gcs_client
        gcs_reader = GCS(bucket='test')
        gcs_reader._filter = mock.Mock(return_value=[])
        extract_response = gcs_reader._extract_raw_data()
        gcs_reader._filter.assert_called_with()
        self.assertEqual(None, extract_response)

    @mock.patch('avroconvert.sources.gcs.reader.storage')
    def test_gcs_w_no_bucket(self, mock_strg):
        gcs_client = mock.MagicMock()
        gcs_client.get_bucket = mock.MagicMock()
        mock_strg.Client.from_service_account_json.return_value = gcs_client
        with self.assertRaises(AttributeError) as e:
            gcs_reader = GCS(bucket=None)

    @mock.patch('avroconvert.sources.gcs.reader.storage')
    def test_gcs_w_no_auth(self, mock_strg):
        del environ['GOOGLE_APPLICATION_CREDENTIALS']
        gcs_client = mock.MagicMock()
        gcs_client.get_bucket = mock.MagicMock()
        mock_strg.Client.from_service_account_json.return_value = gcs_client
        with self.assertRaises(AttributeError) as e:
            gcs_reader = GCS(bucket='test')
        self.assertEqual(('Credentials not set. Please set GOOGLE_APPLICATION_CREDENTIALS or pass the path of service account json file',),
                         e.exception.args)
        environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'test-service.json'

    @mock.patch('avroconvert.sources.gcs.reader.storage')
    def test_read_files(self, mock_strg):
        gcs_client = mock.MagicMock()
        gcs_client.get_bucket = mock.MagicMock()
        gcs_client.get_bucket.get_blob = mock.MagicMock()
        gcs_client.get_bucket.get_blob.download_as_string = mock.MagicMock(
            return_value=b'test-data')
        mock_strg.Client.from_service_account_json.return_value = gcs_client
        gcs_reader = GCS(bucket='test')
        res = gcs_reader._read_files(filename='test.avro')

    @mock.patch('avroconvert.sources.gcs.reader.storage')
    def test_get_data(self, mock_strg):
        gcs_client = mock.MagicMock()
        gcs_client.get_bucket = mock.MagicMock()
        mock_strg.Client.from_service_account_json.return_value = gcs_client
        gcs_reader = GCS(bucket='test')
        gcs_reader._extract_raw_data = mock.MagicMock(
            return_value={'file1.avro': b'data1', 'file2.avro': b'data2'})
        get_data_response = gcs_reader.get_data()
        self.assertEqual(
            {'file1.avro': b'data1', 'file2.avro': b'data2'}, get_data_response)

    @mock.patch('avroconvert.sources.gcs.reader.storage')
    def test_get_data_wrong_format(self, mock_strg):
        gcs_client = mock.MagicMock()
        gcs_client.get_bucket = mock.MagicMock()
        mock_strg.Client.from_service_account_json.return_value = gcs_client
        gcs_reader = GCS(bucket='test', datatype='random')
        gcs_reader._extract_raw_data = mock.MagicMock(
            return_value={'file1.avro': b'data1', 'file2.avro': b'data2'})
        with self.assertRaises(TypeError) as e:
            get_data_response = gcs_reader.get_data()
        self.assertEqual(
            ('Given datatype random not supported yet',), e.exception.args)

    @classmethod
    def tearDownClass(cls):
        pass
