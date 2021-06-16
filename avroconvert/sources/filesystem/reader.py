from os import path, walk
from avroconvert import logger


class FileSystem:
    '''
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

    def __init__(self, bucket: str, prefix: str = None, datatype: str = 'avro'):
        self.folder = bucket
        self.prefix = prefix
        self.datatype = datatype

    def get_data(self):
        '''
        Lists all files in the local folder (filtering by prefix if one is provided), 
        reads them as bytes, and returns a list of data read from the files.

        :returns: list of bytes where each element of the list represents a 
                  file read (in bytes) from local folders.
        :rtype: list
        '''
        supported_types = ['avro']
        if self.datatype not in supported_types:
            raise TypeError(
                f'Given datatype {self.datatype} not supported yet!')
        if self.prefix:
            filelist = [path.join(path1, ea_file) for path1, currentDirectory, files in walk(self.folder)
                        for ea_file in files if ea_file.startswith(self.prefix)]
        else:
            filelist = [path.join(path1, ea_file) for path1, currentDirectory, files in walk(self.folder)
                        for ea_file in files]

        records = {filename: self.read_files(filename=filename) for filename in filelist if filename.endswith('.avro')}
        return records

    def read_files(self, filename: str):
        '''
        Read file from local filesystem and convert it
        into bytes

        :param filename: Name of the file to read from local folder
        :type filename: str

        :returns: avro file from local folder, converted to bytes
        :rtype: bytes
        '''
        logger.info(f'Reading file {filename} from filesystem in bytes')
        with open(filename, 'rb') as f:
            data = f.read()
        return data
