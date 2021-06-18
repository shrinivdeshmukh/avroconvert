"""Console script for avroconvert."""
import argparse
import sys
import time

from avroconvert import Execute


def main():
    """Console script for avroconvert."""
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest='command')

    gs_parser = subparsers.add_parser(
        'gs', help='read files from google cloud storage')
    gs_parser.add_argument('--auth-file', nargs='?', required=False,
                           help='path of the google\'s service account file')
    
    gs_parser.add_argument('-b', '--bucket', nargs='?', required=True,
                        help='Name of the bucket in the \
                              google cloud storage ')
    gs_parser.add_argument('-p', '--prefix', nargs='?', required=False, default='',
                        help='File prefix; files starting with this prefix \
                            value will be read, converted and stored. \
                            All other files will be omitted')
    gs_parser.add_argument('-o', '--outfolder', nargs='?', required=True,
                        help='Output folder; all the output files will be \
                            stored at this folder location')
    gs_parser.add_argument('-f', '--format', nargs='?', required=True,
                        choices=['parquet', 'csv', 'json'],
                        help='Output format; avro files will be converted to this format')

    s3_parser = subparsers.add_parser(
        's3', help='read files from amazon s3 storage')
    
    s3_parser.add_argument('--access-key', nargs='?', required=False,
                           help='AWS access key; It is required only if AWS is \
                               not configured or the file ~/.aws/credentials does not exist')

    s3_parser.add_argument('--secret-key', nargs='?', required=False,
                           help='AWS secret key; It is required only if AWS is \
                               not configured or the file ~/.aws/credentials does not exist')

    s3_parser.add_argument('--session-token', nargs='?', required=False,
                           help='AWS session token; It is required only if AWS is \
                               not configured or the file ~/.aws/credentials does not exist')
    
    s3_parser.add_argument('-b', '--bucket', nargs='?', required=True,
                        help='Name of the bucket amazon s3 storage')
    s3_parser.add_argument('-p', '--prefix', nargs='?', required=False, default='',
                        help='File prefix; files starting with this prefix \
                            value will be read, converted and stored. \
                            All other files will be omitted')
    s3_parser.add_argument('-o', '--outfolder', nargs='?', required=True,
                        help='Output folder; all the output files will be \
                            stored at this folder location')
    s3_parser.add_argument('-f', '--format', nargs='?', required=True,
                        choices=['parquet', 'csv', 'json'],
                        help='Output format; avro files will be converted to this format')

    fs_parser = subparsers.add_parser(
        'fs', help='read files from local file system')

    fs_parser.add_argument('-i', '--input-dir', nargs='?', required=True,
                        help='Name/path of the input directory. This \
                            directory should contain the avro files')
    fs_parser.add_argument('-p', '--prefix', nargs='?', required=False, default='',
                        help='File prefix; files starting with this prefix \
                            value will be read, converted and stored. \
                            All other files will be omitted')
    fs_parser.add_argument('-o', '--outfolder', nargs='?', required=True,
                        help='Output folder; all the output files will be \
                            stored at this folder location')
    fs_parser.add_argument('-f', '--format', nargs='?', required=True,
                        choices=['parquet', 'csv', 'json'],
                        help='Output format; avro files will be converted to this format')


    args = parser.parse_args()
    if args.command == 'gs':
        executor = Execute(source='gs', bucket=args.bucket, dst_format=args.format,
                           prefix=args.prefix, auth_file=args.auth_file,
                           outfolder=args.outfolder)
    elif args.command == 's3':
        executor = Execute(source='s3', bucket=args.bucket, dst_format=args.format,
                           prefix=args.prefix, access_key=args.access_key,
                           secret_key=args.secret_key, session_token=args.session_token,
                           outfolder=args.outfolder)
    elif args.command == 'fs':
        executor = Execute(source='fs', bucket=args.input_dir, dst_format=args.format,
                           prefix=args.prefix, outfolder=args.outfolder)
    else:
        print('You must supply a source from gs, s3 or fs\n', file=sys.stderr)
        parser.print_help()
        return
    start_time = time.time()
    executor.run()
    end_time = time.time()
    # print("Arguments: " + args)
    print(f"Conversion completed in {end_time - start_time} seconds!")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
