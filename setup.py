#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'boto3==1.15.15',
    'fastavro==1.0.0.post1',
    'pandas==1.2.0',
    'pyarrow==4.0.1',
    'google-api-core==1.22.4',
    'google-api-python-client==1.12.8',
    'google-auth==1.22.1',
    'google-auth-httplib2==0.0.4',
    'google-auth-oauthlib==0.4.2',
    'google-cloud-bigquery==2.0.0',
    'google-cloud-bigquery-storage==2.0.0',
    'google-cloud-core==1.4.2',
    'google-cloud-storage==1.31.2',
    'google-crc32c==1.0.0',
    'google-resumable-media==1.1.0',
    'googleapis-common-protos==1.52.0',
    'pandas==1.2.0',
    'pyarrow==4.0.1',
    'pydata-google-auth==1.1.0',
    'python-snappy==0.6.0'
]

test_requirements = [ ]

setup(
    author="Shrinivas Vijay Deshmukh",
    author_email='shrinivas.deshmukh11@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Utility to convert avro files to csv, json and parquet formats",
    entry_points={
        'console_scripts': [
            'avroconvert=avroconvert.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme, #+ '\n\n' + history,
    include_package_data=True,
    keywords='avroconvert',
    name='avroconvert',
    packages=find_packages(include=['avroconvert', 'avroconvert.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/shrinivdeshmukh/avroconvert',
    version='0.1.0',
    zip_safe=False,
)
