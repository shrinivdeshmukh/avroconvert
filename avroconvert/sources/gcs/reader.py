from google.cloud import storage
from os import getenv, path
from avroconvert import logger


class GCS:
    '''
    A class used to read files from google cloud storage and
    convert them to list of bytes

    :param client: 
        google cloud storage bucket client object. With this,
        we can interact with google cloud's bucket
    :type client: gcs bucket client

    :param bucket: Name of the bucket in google cloud storage. This is
                   where the avro file is read from
    :type bucket: str

    :param datatype: format of the source file, which will be read
                     from google cloud storage. Default value is `avro`
    :type datatype: str

    :param prefix: str
        prefix is the starting letters of the file names
        in the cloud storage. For example, if the bucket
        contains files with name `test-01`, `test-02` and `test-03`,
        the file prefix can be `test`. All the files with this
        prefix will be read
    :type prefix: str
    '''

    def __init__(self, auth_file: str = None, bucket: str = None, datatype: str = 'avro', prefix: str = None):
        '''
        :param auth_file: path to the google cloud service account json file
        :type auth_file: str

        :param bucket:  Name of the bucket in google cloud storage. This is
                 where the avro file is read from
        :type bucket: str

        :param datatype: str
            format of the source file, which will be read
            from google cloud storage. Default value is `avro`
        :type datatype: str

        :param prefix: str
            prefix is the starting letters of the file names
            in the cloud storage. For example, if the bucket
            contains files with name `test-01`, `test-02` and `test-03`,
            the file prefix can be `test`. All the files with this
            prefix will be read
        :type prefix: str
        '''
        self.client = self._auth(auth_file=auth_file, bucket=bucket)
        self.bucket = bucket
        logger.debug(f'Bucket name as received is {self.bucket}')

        self.datatype = datatype
        logger.debug(f'File datatype as received is {self.datatype}')

        self.prefix = prefix
        logger.debug(f'File prefix as received is {self.prefix}')

    def _auth(self, auth_file: str = None, bucket: str = None):
        '''
        This method authenticates the code to interact with 
        google cloud, and creates the client object.

        :param auth_file: path to the google cloud service 
                          account json file
        :type bucket: str

        :param bucket:  Name of the bucket in google cloud storage. This is
                        where the avro file is read from
        :type bucket: str

        :returns: the client object is used to interact with google
            cloud storage
        :rtype : google storage client object
        '''
        if not getenv('BUCKET', bucket):
            raise AttributeError('Please pass the GCS bucket name')

        if not getenv('GOOGLE_APPLICATION_CREDENTIALS', auth_file):
            err = 'Credentials not set. Please set GOOGLE_APPLICATION_CREDENTIALS or'
            err += ' pass the path of service account json file'
            raise AttributeError(err)

        gcs_client = storage.Client.from_service_account_json(
            getenv('GOOGLE_APPLICATION_CREDENTIALS', auth_file))

        return gcs_client.get_bucket(getenv('BUCKET', bucket))

    def _extract_raw_data(self) -> list:
        '''
        It lists all the files in google storage bucket
        starting with a prefix (if prefix is passed). It
        then calls another method called `read_files` to
        read each file

        :returns: dictionary of bytes where each key of the dict is
                  the file name of the input file and it's value is 
                  the data read (as bytes) from that file
        :rtype: dict
        '''
        logger.info('Listing files in GCS')
        gcs_files = self._filter()
        if not gcs_files:
            logger.info(f'No files with prefix {self.prefix} found in GCS')
            return None

        data = {gcs_file: self._read_files(filename=gcs_file)
                for gcs_file in gcs_files if gcs_file.endswith('.avro')}
        return data

    def _filter(self):
        '''
        Helper function to avoid reading empty folders
        from google storage buckets
        '''
        files = list()

        for blob in self.client.list_blobs(prefix=self.prefix):
            try:
                filename = blob.name
                if filename.split('/')[1] == '' \
                        and path.dirname(filename) == filename.split('/')[0]:
                    continue
                else:
                    files.append(filename)
            except IndexError as e:
                files.append(filename)
                continue
        return files

    def _read_files(self, filename: str) -> bytes:
        '''
        Read file from google cloud bucket and convert it
        into bytes

        :param filename: Name of the file to read from google 
                         cloud bucket
        :type filename: str

        :returns: avro file from google, converted to bytes
        :rtype: bytes
        '''
        logger.info(f'Reading file {filename} from GCS in bytes')
        gcs_blob = self.client.get_blob(filename)
        raw_data = gcs_blob.download_as_string()
        return raw_data

    def get_data(self) -> list:
        '''
        Lists all files in S3 (filtering by prefix if one is provided), 
        reads them as bytes, and returns a list of data read from the files.

        :returns: list of bytes where each element of the list represents a 
                  file read (in bytes) from Google Storage Bucket.
        :rtype: list
        '''
        supported_types = ['avro']
        if self.datatype not in supported_types:
            raise TypeError(
                f'Given datatype {self.datatype} not supported yet')

        raw_data_dict = self._extract_raw_data()
        return raw_data_dict
