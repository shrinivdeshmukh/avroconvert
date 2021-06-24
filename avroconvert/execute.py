import avroconvert as avc
from multiprocessing import cpu_count
import concurrent


class Execute:

    def __init__(self, source: str, bucket: str, dst_format: str, outfolder: str, prefix: str = '', **kwargs):
        '''
        A wrapper class to run the avro convert operation. This class
        calls the reader methods (gcs, s3 or local) and avro converter
        methods internally.

        :param source: Name of the source file system. Should be one
                       of these: gs, s3 of fs. 
                       gs is for google cloud bucket
                       s3 is for amazon s3 bucket
                       fs is for local file system
        :type source: str

        :param bucket: Name of the bucket to read the files. For local
                      file system, bucket would be the folder name from where
                      the data will be read and converted to specified
                      output format
        :type bucket: str

        :param dst_format: Target output format. The files read from
                          different sources will be converted to the
                          format specified by this parameter. It's
                          value should be one of these: 
                          cs, parquet or json, defaults to parquet
        :type dst_format: str

        :param outfolder: Output folder. This is where the files
                         converted from avro to csv, parquet or json
                         will be stored
        :type outfolder: str

        :param prefix: File prefix. If given, files whose names start with
                      the given prefix will be read and all the other
                      files will be omitted
        :type prefix: str

        :key auth_file: Pass this parameter only when the source is `gs`.
                       It specifies the location of service account json
                       file to access google cloud storage. If google
                       cloud is authenticated or the environment
                       variable GOOGLE_APPLICATION_CREDENTIALS is set
                       in the already, then this parameter is not 
                       required

        :key access_key: Pass this parameter only when the source is `s3`. 
                         It specifies AWS access key id. If aws is already 
                         authenticated or there exists a file ~/.aws/credentials
                         or the environment variable AWS_ACCESS_KEY_ID is set,
                         then this parameter is not required

        :key secret_key: Pass this parameter only when the source is `s3`. 
                         It specifies AWS secret key. If aws is already 
                         authenticated or there exists a file ~/.aws/credentials
                         or the environment variable AWS_SECRET_ACCESS_KEY is set,
                         then this parameter is not required

        :key session_token: Pass this parameter only when the source is `s3`. 
                           It specifies AWS session token.
        '''
        _src = ['s3', 'gs', 'fs']
        _dst_format = ['parquet', 'csv', 'json']
        source = source.lower()
        if not dst_format:
            raise AttributeError(f'Output format not specified, should be one of {_dst_format}')
        if not outfolder:
            raise AttributeError(f'Please specify an output folder')
        dst_format = dst_format.lower()
        if source not in _src:
            raise Exception(
                f'Invalid source {source} passed. Source should be one of {_src}')
        if dst_format not in _dst_format:
            raise Exception(
                f'Invalid format {dst_format}. It should be one of {_dst_format}')
        if not bucket:
            raise Exception(
                f'Please specify a bucket')
        self.source = source
        self.bucket = bucket
        self.prefix = prefix
        self.dst_format = dst_format
        self.outfolder = outfolder
        self.params = kwargs

    def _resolve(self):
        '''
        This method returns a reader instance depending upon
        the source specified. If the source is `gs`, this
        method will return `gs_reader`; if the source is `s3`,
        this method will return `s3_reader`; if the source is
        `fs`, this method will return `fs_reader` object
        '''
        reader_function = getattr(avc, f'{self.source}_reader')
        reader = reader_function(
            bucket=self.bucket, prefix=self.prefix, **self.params)
        return reader

    def run(self) -> bool:
        '''
        Executor method for the AvroConverter class. This method
        parallelizes the execution for all the file read->convert->write operations.
        '''
        raw_content = self._resolve().get_data()
        if not raw_content:
            return 
        num_process = cpu_count()*2
        avro_object = avc.AvroConvert(
            dst_format=self.dst_format, outfolder=self.outfolder)
        with concurrent.futures.ProcessPoolExecutor(max_workers=int(num_process)) as executor:
            results = [executor.submit(
                avro_object.convert_avro, **{'filename': filename, 'data': avrodata}) for filename, avrodata in raw_content.items()]
        return True
