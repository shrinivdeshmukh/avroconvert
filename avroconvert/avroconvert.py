"""Main module."""
from avroconvert import logger
import csv
from fastavro import reader
from io import BytesIO
from itertools import chain
from json import dump
from os.path import join, exists, dirname
from pandas import DataFrame
from pathlib import Path
from pyarrow import csv as pac, Table
from pyarrow.parquet import write_table
from tempfile import mkstemp, TemporaryDirectory


class AvroConvert:
    '''
    A class used to read avro files and convert them to csv,
    parquet and json format

    :param header: Extracts header from the file if it is set to True
    :type header: bool

    :param dst_format: Specifies the format to convert the avro data to
    :type dst_format: str

    :param data: Contains raw data in the form of bytes as read from 
                filesystem, google cloud storage or S3. Multiple 
                files are read sequentially and their respective data
                is appended to this list which is passed as the
                variable `data`
    :type data: list
    '''

    def __init__(self, data: list, dst_format: str = 'csv', header: bool = False):
        """
        :param header: Extracts header from the file if it is set to True
        :type header: bool

        :param dst_format: Specifies the format to convert the avro data to
        :type dst_format: str

        :param data: Contains raw data in the form of bytes as read from 
                    filesystem, google cloud storage or S3. Multiple 
                    files are read sequentially and their respective data
                    is appended to this list which is passed as the
                    variable `data`
        :type data: list
        """
        self.header = header
        self.dst_format = dst_format
        self.data = self.read_avro(data)

    def read_avro(self, data: list) -> list:
        '''
        Reads bytes data and converts it to avro format

        :param data: Contains raw data in the form of bytes as read from 
                    filesystem, google cloud storage or S3. Multiple 
                    files are read sequentially and their respective data
                    is appended to this list which is passed as the
                    variable `data`
        :type data: list

        :returns: list containing avro data
        :rtype: list
        '''
        logger.info('Converting bytes to avro')
        records = list()
        logger.debug(f'Data from total {len(data)} read')
        while len(data) > 0:
            logger.debug('Reading data from avro generator object')
            record = [r for r in reader(BytesIO(data.pop()))]
            records.append(record)
        return records

    def to_csv(self, outfile: str) -> str:
        '''
        Write the avro data to a csv file
        :param outfile: Output filepath. The avro data which is 
                        converted to csv, will be stored at this location. 
                        If a non-existent folder name is given, 
                        the folder will be created and the csv file will 
                        be written there.
                        Example: ./data/1970-01-01/FILE.csv

        :returns: path of the output csv file
        :rtype: str
        '''
        count = 0
        self._check_output_folder(outfile)
        f = csv.writer(open(outfile, "w+"))
        while len(self.data) > 0:
            record = self.data.pop()
            for row in record:
                if self.header == True:
                    header = row.keys()
                    f.writerow(header)
                    self.header = False
                count += 1
                f.writerow(row.values())
        return outfile

    def to_parquet(self, outfile: str) -> str:
        '''
        Write the avro data to a parquet file

        :param outfile: Output filepath. The avro data which is converted to 
                        parquet, will be stored at this location. If a non-existent 
                        folder name is given, the folder will be created and the
                        parquet file will be written there.
                        Example: ./data/1970-01-01/FILE.parquet
        :type outfile: str

        :returns: path of the output parquet file
        :rtype: str
        '''
        self._check_output_folder(outfile)
        # TODO: support for partitioned storage
        table = Table.from_pandas(
            DataFrame(list(chain.from_iterable(self.data))))
        write_table(table, outfile, flavor='spark')
        return outfile

    def to_json(self, outfile: str) -> str:
        '''
        Write the avro data to a json file
        :param outfile: Output filepath. The avro data which is converted to 
                        json, will be stored at this location. If a non-existent 
                        folder name is given, the folder will be created and the
                        json file will be written there.
                        Example: ./data/1970-01-01/FILE.json
        :type outfile: str

        :returns: path of the output json file
        :rtype: str
        '''
        self._check_output_folder(outfile)
        df = DataFrame()
        while len(self.data) > 0:
            df = df.append(self.data.pop())
        df.to_json(outfile, orient='records')
        return outfile

    def _check_output_folder(self, folderpath: str) -> bool:
        '''
        :param folderpath: output file path. It is used to 
                           check if the path exists or not.
                           If not, the folders are created
        :type folderpath: str

        :returns: True
        :rtype: bool
        '''
        folderpath = dirname(folderpath)
        if not exists(folderpath):
            Path(folderpath).mkdir(parents=True, exist_ok=True)
        return True
