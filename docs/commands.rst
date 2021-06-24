========
Commands
========

Introduction
============

`avroconvert` provides a very user-friendly cli for interacting with the tool.
The parameters can be specified on the command line or in a configuration file (.ini).

Read avro files from google storage bucket
==========================================

The command :code:`avroconvert gs` sets the source to google cloud bucket. The parameters
supported are:

    - :code:`--auth-file`: :code:`optional`
        - If Google Cloud authentication has not yet been completed, this argument can be used to pass the service account json/p12 file.
        - Example: :code:`avroconvert gs --auth-file gcs.json`

    - :code:`-b, --bucket`: :code:`required`
        - Bucket name can be passed using this argument.
        - Example: :code:`avroconvert gs -b test-bucket`

    - :code:`-p, --prefix`: :code:`optional`
        - The name prefix of the files that will be read from the bucket. Only the files that begin with the prefix will be read; the rest will be ignored.
        - Example: :code:`avroconvert gs -b test-bucket -p data/test-2021-`


    - :code:`-f,--format`: :code:`required`
        - This is the output format; the input avro files will be converted to it. Currently, parquet, csv, and json are supported formats.
        - Example: :code:`avroconvert gs -b test-bucket -f parquet -p data/test-2021-`

    - :code:`-o,--outfolder`: :code:`required`
        - The destination folder for the converted files. If the folder does not already exist, it will be created.
        - Example: :code:`avroconvert gs -b test-bucket -f parquet -p data/test-2021 -o output-data-folder/`

    - :code:`--config`: :code:`optional`
        - All of the above parameters can be written to a configuration file, which can then be passed as an argument. The cli argument will be used when a parameter is written in the configuration file and also passed via command line arguments. The configuration file syntax is given at the end of this page.
        - Example: :code:`avroconvert gs -b test-bucket --config ./config.ini`

Read avro files from amazon S3 bucket
==========================================

The command :code:`avroconvert s3` sets the source to amazon s3 bucket. The parameters
supported are:

    - :code:`--access-key`: :code:`optional`
        - If aws authentication hasn't been done yet, this argument can be used to pass the aws access key.
        - Example: :code:`avroconvert s3 --access-key some-access-key`

    - :code:`--secret-key`: :code:`optional`
        - If aws authentication hasn't been done yet, this argument can be used to pass the aws secret key.
        - Example: :code:`avroconvert s3 --secret-key some-secret-key`

    - :code:`--session-token`: :code:`optional`
        - If aws session token is required, then it can be passed using this argument.
        - Example: :code:`avroconvert s3 --session-token some-session-token`

    - :code:`-b, --bucket`: :code:`required`
        - Bucket name can be passed using this argument.
        - Example: :code:`avroconvert s3 -b test-bucket`

    - :code:`-p, --prefix`: :code:`optional`
        - The name prefix of the files that will be read from the bucket. Only the files that begin with the prefix will be read; the rest will be ignored.
        - Example: :code:`avroconvert s3 -b test-bucket -p data/test-2021-`


    - :code:`-f,--format`: :code:`required`
        - This is the output format; the input avro files will be converted to it. Currently, parquet, csv, and json are supported formats.
        - Example: :code:`avroconvert s3 -b test-bucket -f parquet -p data/test-2021-`

    - :code:`-o,--outfolder`: :code:`required`
        - The destination folder for the converted files. If the folder does not already exist, it will be created.
        - Example: :code:`avroconvert s3 -b test-bucket -f parquet -p data/test-2021 -o output-data-folder/`
    
    - :code:`--config`: :code:`optional`
        - All of the above parameters can be written to a configuration file, which can then be passed as an argument. The cli argument will be used when a parameter is written in the configuration file and also passed via command line arguments. The configuration file syntax is given at the end of this page.
        - Example: :code:`avroconvert s3 -b test-bucket --config ./config.ini`

Read avro files from local filesystem
==========================================

The command :code:`avroconvert fs` sets the source to local filesystem. The parameters
supported are:

    - :code:`-i, --input-dir`: :code:`required`
        - This argument can be used to specify the input directory containing the avro files.
        - Example: :code:`avroconvert fs -i input_data/`

    - :code:`-p, --prefix`: :code:`optional`
        - The name prefix of the files that will be read from the input directory. Only the files that begin with the prefix will be read; the rest will be ignored.
        - Example: :code:`avroconvert fs -i input_data/ -p data/test-2021-`


    - :code:`-f,--format`: :code:`required`
        - This is the output format; the input avro files will be converted to it. Currently, parquet, csv, and json are supported formats.
        - Example: :code:`avroconvert fs -i input_data/ -f parquet -p data/test-2021-`

    - :code:`-o,--outfolder`: :code:`required`
        - The destination folder for the converted files. If the folder does not already exist, it will be created.
        - Example: :code:`avroconvert fs -i input_data/ -f parquet -p data/test-2021 -o output-data-folder/`
    
    - :code:`--config`: :code:`optional`
        - All of the above parameters can be written to a configuration file, which can then be passed as an argument. The cli argument will be used when a parameter is written in the configuration file and also passed via command line arguments. The configuration file syntax is given at the end of this page.
        - Example: :code:`avroconvert fs -i input_data/ --config ./config.ini`

Configuration File
==================

.. code-block:: ini

    [gs]
    auth_file =
    bucket =
    prefix =
    format =
    outfolder =

    [s3]
    access_key = 
    secret_key = 
    session_token = 
    bucket = 
    prefix = 
    format = 
    outfolder = 

    [fs]
    input_dir = 
    prefix = 
    format = 
    outfolder = 

We have three sections in the file above for the three sources that the tool currently supports, 
which are Google Storage Bucket, Amazon S3, and the local filesystem.

Each of the parameters we discussed earlier can be written into this configuration file, which can then be passed to the tool as an argument. 
`avroconvert` will receive all of the parameters from this file.

**NOTE** `The arguments passed via cli take precedence over the parameters specified in the configuration file. The cli parameter will be used if a parameter is written in the configuration file and also passed via cli.`