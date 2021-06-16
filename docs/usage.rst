=====
Usage
=====

To use avroconvert in a project::

    from avroconvert.execute import Execute

    # for google storage bucket as the source
    output = Execute(source='gs', bucket='<BUCKET_NAME>, dst_format='parquet', auth_file='<SERVICE_ACCOUNT.json>',
                     outfile='OUTPUT_FOLDER').run()

    # for amazon s3 storage bucket reader
    output = Execute(source='gs', bucket='<BUCKET_NAME>, dst_format='parquet', auth_file='<SERVICE_ACCOUNT.json>',
                     outfile='OUTPUT_FOLDER', access_key='<AWS ACCESS KEY>', secret_key='<AWS SECRET KEY>', 
                     session_token='<AWS SESSION TOKEN>(if any)', bucket='<S3 BUCKET>', prefix='<FILE PREFIX>').run()

    # google storage bucket reader
    output = Execute(source='gs', bucket='<BUCKET_NAME>, dst_format='parquet', auth_file='<SERVICE_ACCOUNT.json>',
                     outfile='OUTPUT_FOLDER').run()

    # Local file system reader
    output = Execute(source='fs', bucket='<LOCAL_FOLDER NAME> dst_format='parquet').run()
