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
from pyarrow import Table
from pyarrow.parquet import write_table

class AvroConvert:
    '''
    A class used to read avro files and convert them to csv,
    parquet and json format

    :param outfolder: output folder to write the output files
                     to
    :type outfolder: str

    :param header: Extracts header from the file if it is set to True
    :type header: bool

    :param dst_format: Specifies the format to convert the avro data to
    :type dst_format: str
    '''

    def __init__(self, outfolder: str, dst_format: str = 'parquet', header: bool = True):
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
        self.dst_format = dst_format.lower()
        # self.data = data
        self.outfolder = outfolder
        self._check_output_folder(outfolder)

    def convert_avro(self, filename: str, data: bytes) -> str:
        '''
        Reads byte data, converts it to avro format and writes
        the data to the local filesystem to the output format
        specified.

        :param filename: Name of the input file (with it's source path). 
                        The output file will be saved by the same name,
                        within the same folder hierarchy as it was in 
                        the source file system. The extension will be 
                        changed as per the given output format
        :type filename: str

        :param data: Contains raw data in the form of bytes as read from 
                    filesystem, google cloud storage or S3. Multiple 
                    files are read sequentially and their respective data
                    is appended to this list which is passed as the
                    variable `data`
        :type data: bytes

        :returns: File name with path of the output file
        :rtype: str
        '''
        if not bool(data):
            return None
        try:
            logger.info('Converting bytes to avro')
            logger.info(f'File {filename} in progress')
            outfile = join(self.outfolder, self._change_file_extn(filename))
            avrodata = [r for r in reader(BytesIO(data))]
            logger.info(
                f'Total {len(avrodata)} records found in file is {filename}')

            writer_function = getattr(self, f'_to_{self.dst_format}')
            writer_function(data=avrodata, outfile=outfile)
            logger.info(f'[COMPLETED] File {outfile} complete')
            return f'File {outfile} complete'
        except Exception as e:
            logger.exception(f'[FAILED] File {outfile} failed')
            raise e

    def _to_csv(self, data, outfile: str) -> str:
        '''
        Write the avro data to a csv file

        :param data: Avro formatted data
        :type data: avro data

        :param outfile: Output filepath. The avro data which is 
                        converted to csv, will be stored at this location. 
                        If a non-existent folder name is given, 
                        the folder will be created and the csv file will 
                        be written there.
                        Example: ./data/1970-01-01/FILE.csv
        :type outfile: str

        :returns: path of the output csv file
        :rtype: str
        '''
        count = 0
        logger.info(f'Output folder check {outfile}')
        self._check_output_folder(outfile)
        f = csv.writer(open(outfile, "w+"))
        for row in data:
            if self.header == True:
                header = row.keys()
                f.writerow(header)
                self.header = False
            count += 1
            f.writerow(row.values())
        return outfile

    def _to_parquet(self, data, outfile: str) -> str:
        '''
        Write the avro data to a parquet file

        :param data: Avro formatted data
        :type data: avro data
        
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
        # table = Table.from_pandas(
        #     DataFrame(list(chain.from_iterable(self.data))))
        logger.info(f'Writing {outfile} to parquet format')
        try:
            table = Table.from_pandas(
                DataFrame(data))
            write_table(table, outfile, flavor='spark')
            return outfile
        except Exception as e:
            raise e

    def _to_json(self, data, outfile: str) -> str:
        '''
        Write the avro data to a json file
        
        :param data: Avro formatted data
        :type data: avro data

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
        df = DataFrame(data)
        # while len(self.data) > 0:
        # df = df.append(self.data.pop())
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
        # folderpath = dirname(folderpath)
        if dirname(folderpath):
            folderpath = dirname(folderpath)
        if not exists(folderpath):
            logger.info(f'Path {folderpath} does not exist; creating new folder')
            Path(folderpath).mkdir(parents=True, exist_ok=True)
        return True

    def _change_file_extn(self, filename: str) -> str:
        '''
        Change the input file extension to given
        output format

        :param filename: name of the input file with .avro
                         extension
        :type filename: str

        :returns: name of the output file with output file
                  extension
        :rtype: str
        '''
        p = Path(filename)
        new_filename = p.parent.joinpath(f'{p.stem}.{self.dst_format}')
        new_filename = str(new_filename)
        return new_filename