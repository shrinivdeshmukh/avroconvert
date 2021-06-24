"""Console script for avroconvert."""
import argparse
import configparser
import sys
import time

from avroconvert import Execute

def get_config_option(config: configparser.ConfigParser, section: str, option: str):
    try:
        return config.get(section, option)
    except configparser.NoSectionError:
        return None
    except configparser.NoOptionError:
        return None

def main():
    """Console script for avroconvert."""
    parser = argparse.ArgumentParser()
    config = configparser.ConfigParser()
    subparsers = parser.add_subparsers(dest='command')

    gs_parser = subparsers.add_parser(
        'gs', help='read files from google cloud storage')
    gs_parser.add_argument('--auth-file', nargs='?', 
                           help='path of the google\'s service account file')
    
    gs_parser.add_argument('-b', '--bucket', nargs='?', 
                        help='Name of the bucket in the \
                              google cloud storage ')
    gs_parser.add_argument('-p', '--prefix', nargs='?',  default='',
                        help='File prefix; files starting with this prefix \
                            value will be read, converted and stored. \
                            All other files will be omitted')
    gs_parser.add_argument('-o', '--outfolder', nargs='?', 
                        help='Output folder; all the output files will be \
                            stored at this folder location')
    gs_parser.add_argument('-f', '--format', nargs='?', 
                        choices=['parquet', 'csv', 'json'],
                        help='Output format; avro files will be converted to this format')
    gs_parser.add_argument('--config', nargs=1,  help='configuration file path')

    s3_parser = subparsers.add_parser(
        's3', help='read files from amazon s3 storage')
    
    s3_parser.add_argument('--access-key', nargs='?', 
                           help='AWS access key; It is required only if AWS is \
                               not configured or the file ~/.aws/credentials does not exist')

    s3_parser.add_argument('--secret-key', nargs='?', 
                           help='AWS secret key; It is required only if AWS is \
                               not configured or the file ~/.aws/credentials does not exist')

    s3_parser.add_argument('--session-token', nargs='?', 
                           help='AWS session token; It is required only if AWS is \
                               not configured or the file ~/.aws/credentials does not exist')
    
    s3_parser.add_argument('-b', '--bucket', nargs='?', 
                        help='Name of the bucket amazon s3 storage')
    s3_parser.add_argument('-p', '--prefix', nargs='?', default='',
                        help='File prefix; files starting with this prefix \
                            value will be read, converted and stored. \
                            All other files will be omitted')
    s3_parser.add_argument('-o', '--outfolder', nargs='?', 
                        help='Output folder; all the output files will be \
                            stored at this folder location')
    s3_parser.add_argument('-f', '--format', nargs='?', 
                        choices=['parquet', 'csv', 'json'],
                        help='Output format; avro files will be converted to this format')
    s3_parser.add_argument('--config', nargs=1,  help='configuration file path')

    fs_parser = subparsers.add_parser(
        'fs', help='read files from local file system')

    fs_parser.add_argument('-i', '--input-dir', nargs='?', 
                        help='Name/path of the input directory. This \
                            directory should contain the avro files')
    fs_parser.add_argument('-p', '--prefix', nargs='?',  default='',
                        help='File prefix; files starting with this prefix \
                            value will be read, converted and stored. \
                            All other files will be omitted')
    fs_parser.add_argument('-o', '--outfolder', nargs='?', 
                        help='Output folder; all the output files will be \
                            stored at this folder location')
    fs_parser.add_argument('-f', '--format', nargs='?', 
                        choices=['parquet', 'csv', 'json'],
                        help='Output format; avro files will be converted to this format')

    fs_parser.add_argument('--config', nargs=1,  help='configuration file path')

    args = parser.parse_args()

    bucket, prefix, dst_format, outfolder = None, '', None, None
    if args.config:
        config.read(args.config)
        bucket = get_config_option(config, args.command, 'bucket')
        prefix = get_config_option(config, args.command, 'prefix')
        dst_format = get_config_option(config, args.command, 'format')
        outfolder = get_config_option(config, args.command, 'outfolder')
        
    if args.format: dst_format = args.format
    if args.prefix: prefix = args.prefix
    if args.outfolder: outfolder = args.outfolder
    
    if not dst_format:
        print('You must supply output format from parquet, csv or json\n', file=sys.stderr)
    if not prefix:
        prefix = ''
    if not outfolder:
        print('You must supply output directory', file=sys.stderr)
        
    if args.command == 'gs':
        auth_file = args.auth_file if args.auth_file else get_config_option(config, args.command, 'auth_file')
        if args.bucket: bucket = args.bucket
        executor = Execute(source='gs', bucket=bucket, dst_format=dst_format,
                           prefix=prefix, auth_file=auth_file,
                           outfolder=outfolder)
    elif args.command == 's3':
        access_key = args.access_key if args.access_key else get_config_option(config, args.command, 'access_key')
        secret_key = args.secret_key if args.secret_key else get_config_option(config, args.command, 'secret_key')
        session_token = args.session_token if args.session_token else get_config_option(config, args.command, 'session_token')
        if args.bucket: bucket = args.bucket
       
        executor = Execute(source='s3', bucket=bucket, dst_format=dst_format,
                           prefix=prefix, access_key=access_key,
                           secret_key=secret_key, session_token=session_token,
                           outfolder=outfolder)
    elif args.command == 'fs':
        input_dir = args.input_dir if args.input_dir else get_config_option(config, args.command, 'input_dir')
        executor = Execute(source='fs', bucket=input_dir, dst_format=dst_format,
                           prefix=prefix, outfolder=outfolder)
    else:
        print('You must supply a source from gs, s3 or fs\n', file=sys.stderr)
        parser.print_help()
        return
    start_time = time.time()
    executor.run()
    end_time = time.time()
    print(f"Conversion completed in {end_time - start_time} seconds!")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
