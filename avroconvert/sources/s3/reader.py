import boto3 as bt
from os import getenv
from avroconvert import logger


class S3:
    '''
    A class used to read files from amazon s3 and
    convert them to bytes

    :param access_key: AWS access key id
    :type access_key: str

    :param secret_key: AWS secret access key
    :type secret_key: str

    :param session_token: AWS session token (if any)
    :type session_token: str

    :param bucket: Name of the bucket in s3. This is
                    where the avro file is read from
    :type bucket: str

    :param datatype: format of the source file, which will be read
                        from s3. Default value is `avro`
    :type datatype: str

    :param prefix: prefix is the starting letters of the file names
                in the cloud storage. For example, if the bucket
                contains files with name `test-01`, `test-02` and `test-03`,
                the file prefix can be `test`. All the files with this
                prefix will be read
    :type prefix: str
    '''

    def __init__(self, access_key: str = None, secret_key: str = None,
                 session_token: str = None, bucket: str = None, prefix: str = '', datatype: str = 'avro'):
        '''

        :param access_key: AWS access key id
        :type access_key: str

        :param secret_key: AWS secret access key
        :type secret_key: str

        :param session_token: AWS session token (if any)
        :type session_token: str

        :param bucket: Name of the bucket in s3. This is
                       where the avro file is read from
        :type bucket: str

        :param datatype: format of the source file, which will be read
                         from s3. Default value is `avro`
        :type datatype: str

        :param prefix: prefix is the starting letters of the file names
                    in the cloud storage. For example, if the bucket
                    contains files with name `test-01`, `test-02` and `test-03`,
                    the file prefix can be `test`. All the files with this
                    prefix will be read
        :type prefix: str
        '''
        self.client = self._auth(access_key, secret_key, session_token, bucket)
        self.bucket = bucket
        logger.debug(f'Bucket name as received is {self.bucket}')

        self.datatype = datatype
        logger.debug(f'File datatype as received is {self.datatype}')

        self.prefix = prefix
        logger.debug(f'File prefix as received is {self.prefix}')

    def _auth(self, access_key: str = None, secret_key: str = None,
              session_token: str = None, bucket: str = None):
        '''
        :param access_key: AWS access key id
        :type access_key: str
        :param secret_key: AWS secret access key
        :type secret_key: str
        :param session_token: AWS session token (if any)
        :type session_token: str
        :param bucket: Name of the bucket in s3. This is 
                       where the avro file is read from
        :type bucket: str

        :returns: amazon s3 bucket client object
        '''
        if not getenv('BUCKET', bucket):
            raise AttributeError('Please pass the S3 bucket name')

        client_params = dict({'aws_access_key_id': getenv('AWS_ACCESS_KEY_ID') or access_key,
                              'aws_secret_access_key': getenv('AWS_SECRET_ACCESS_KEY') or secret_key,
                              'aws_session_token': getenv('AWS_SESSION_TOKEN') or session_token
                              })

        s3_client = bt.resource('s3', **client_params)

        return s3_client.Bucket(getenv('BUCKET', bucket))

    def _extract_raw_data(self) -> dict:
        '''
        It lists all the files in s3 bucket
        starting with a prefix (if prefix is passed). It
        then calls another method called `read_files` to
        read each file

        :returns: dictionary of bytes where each key of the dict is
                  the file name of the input file and it's value is 
                  the data read (as bytes) from that file
        :rtype: dict
        '''
        logger.info('Listing files in S3')
        s3_files = [y.key for y in self.client.objects.filter(
            Prefix=self.prefix)]
        if not s3_files:
            logger.info(f'No files with prefix {self.prefix} found in S3')
            return None
        data = {s3_file: self._read_files(filename=s3_file)
                for s3_file in s3_files if s3_file.endswith('.avro')}
        return data

    def _read_files(self, filename: str) -> bytes:
        '''
        Read file from s3 and convert it into bytes
        :returns: avro file from s3, converted to bytes
        :rtype: bytes
        '''
        logger.info(f'Reading file {filename} from S3 in bytes')
        data_s3_object = self.client.meta\
            .client.get_object(Bucket=self.bucket, Key=filename)
        raw_data = data_s3_object['Body'].read()
        return raw_data

    def get_data(self) -> dict:
        '''
        Lists all files in S3 (filtered by prefix, if it is passed),
        reads the files as bytes and returns a list of data read from 
        the files

        :returns: list all files' data from s3. Each element of the list
                  is bytes
        :rtype: list
        '''
        if self.datatype not in ['avro', 'json', 'csv', 'parquet']:
            return f'Given datatype {self.datatype} not supported yet'
        raw_data_dict = self._extract_raw_data()
        return raw_data_dict
