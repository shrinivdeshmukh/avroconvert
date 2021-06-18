# avroconvert

Utility to convert avro files to csv, json and parquet formats

* ## Installation

Using pypi

```
pip install avroconvert
```

Using git:

```
git clone https://github.com/shrinivdeshmukh/avroconvert
```
```
make install
```

* ## Usage

### Using CLI

CLI can be used to interact with the tool. As the first argument, the source has to be passed. The source can be gs (google cloud storage bucket), s3 (amazon s3 bucket) or fs (local filesystem)

**To read from cloud bucket (google cloud or amazon s3):**

google cloud storage example:

```
avroconvert gs -b <BUCKET_NAME> -f <FORMAT> -o <OUTPUT_FOLDER>
```

amazon s3 example:

```
avroconvert s3 -b <BUCKET_NAME> -f <FORMAT> -o <OUTPUT_FOLDER>
```

The tool reads all avro files from the bucket specified by the `-b` parameter, converts them to the format specified by the `-f` parameter, and writes the output format files to the output folder specified by the `-o` parameter with the above command.

The cli accepts a few additional parameters to authenticate the tool with cloud providers. These parameters are only required if you haven't already been authenticated.

For google cloud, we have `--auth-file`:

```
avroconvert gs -b <BUCKET_NAME> -f <FORMAT> -o <OUTPUT_FOLDER> --auth-file <SERVICE_ACCOUNT_FILE_PATH>.json (or .p12)
```

For amazon s3, we have `--access-key`, `--secret-key`, `--session-token`:

```
avroconvert s3 -b <BUCKET_NAME> -f <FORMAT> -o <OUTPUT_FOLDER> --access-key <AWS_ACCESS_KEY_ID> --secret-key <AWS_SECRET_ACCESS_KEY> --session-token <AWS_SESSION_TOKEN> 
```

**To read from local filesystem**

```
avroconvert fs  -i <INPUT_DATA_FOLDER> -o <OUTPUT_FOLDER> -f <OUTPUT_FORMAT>
```

The tool reads all avro files from the input folder specified by the `-i` parameter, converts them to the format specified by the `-f` parameter, and writes the output format files to the output folder specified by the `-o` parameter with the above command.

**Output folder structure**

The tool replicates the cloud bucket's or local filesystem's directory structure. For example, suppose the output format is parquet and cloud bucket (or local filesystem) has the following structure:

```
BUCKET
├── 2021-06-17
│   └── file1.avro
│   └── file2.avro
│ 
├── 2021-06-16
│   └── data
│       └── file3.avro
│       └── file4.avro

```

the output files will then be saved as:

```
OUTPUT_FOLDER
├── 2021-06-17
│   └── file1.parquet
│   └── file2.parquet
│ 
├── 2021-06-16
│   └── data
│       └── file3.parquet
│       └── file4.parquet

```

**Filter files to read**

A parameter called `-p` or `—-prefix` can be passed as well. All three data sources, gs, s3, and fs, share this parameter. Only files with names that begin with the specified prefix will be read; all other files will be filtered out.

google cloud example with `-p`:

```
avroconvert gs -b <BUCKET_NAME> -f <FORMAT> -o <OUTPUT_FOLDER> -p 2021-06-17/file
```

amazon s3 example with `-p`:

```
avroconvert s3 -b <BUCKET_NAME> -f <FORMAT> -o <OUTPUT_FOLDER> -p 2021-06-17/file
```

local filesystem example with `-p`:

```
avroconvert fs  -i <INPUT_DATA_FOLDER> -o <OUTPUT_FOLDER> -f <OUTPUT_FORMAT> -p 2021-06-17/file
```

### Using the API in code

```
    from avroconvert import Execute

    # for amazon s3 storage bucket reader
    output = Execute(source='gs', bucket='<BUCKET_NAME>, dst_format='parquet', auth_file='<SERVICE_ACCOUNT.json>',
                     outfolder='OUTPUT_FOLDER', access_key='<AWS ACCESS KEY>', secret_key='<AWS SECRET KEY>', 
                     session_token='<AWS SESSION TOKEN>(if any)', bucket='<S3 BUCKET>', prefix='<FILE PREFIX>').run()

    # google storage bucket reader
    output = Execute(source='gs', bucket='<BUCKET_NAME>, dst_format='parquet', auth_file='<SERVICE_ACCOUNT.json>',
                     outfolder='OUTPUT_FOLDER').run()

    # Local file system reader
    output = Execute(source='fs', bucket='<LOCAL_FOLDER NAME> dst_format='parquet', outfolder='OUTPUT_FOLDER').run()

```

* ## Credits

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [audreyr/cookiecutter-pypackage](https://github.com/audreyr/cookiecutter-pypackage) project template.