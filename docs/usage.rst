=====
Usage
=====

To use avroconvert in a project::

    from avroconvert import AvroConvert, gcs_reader, s3_reader, fs_reader

1. Create a reader object::

    # Google cloud storage reader
    reader = gcs_reader(auth_file='<SERVICE_ACCOUNT.json>', bucket='<BUCKET_NAME>', 
                    datatype='avro', prefix='<FILE_PREFIX>')

    # S3 storage reader
    reader = s3_reader(access_key='<AWS ACCESS KEY>', secret_key='<AWS SECRET KEY>', session_token='<AWS SESSION TOKEN>(if any)', 
                   bucket='<S3 BUCKET>', prefix='<FILE PREFIX>', datatype='avro')

    # Local file system reader
    reader = fs_reader(folder='<FOLDER NAME>', prefix='<FILE PREFIX>', datatype='avro')

2. Create avro converter object::

    avro_object = AvroConvert(data=reader.get_data())

3. Write the avro object to parquet, csv or json format::

    avro_object.to_parquet(outfile='<FOLDER/FILENAME.csv>') # to_csv for csv and to_json for json

