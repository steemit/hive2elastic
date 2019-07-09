# coding=utf-8
import sys, os

from setuptools import find_packages
from setuptools import setup

assert sys.version_info[0] == 3 and sys.version_info[1] >= 5, "hive2elastic requires Python 3.5 or newer"

# Get version from the VERSION file
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'VERSION'), encoding='utf-8') as f:
    version = f.read()

setup(
    name='hive2elastic',
    version=version,
    description='hive to elastic exporter',
    long_description=open('README.md').read(),
    packages=find_packages(),
    install_requires=[
        'configargparse',
        'elasticsearch',
        'sqlalchemy',
        'psycopg2-binary',
        'markdown2',
        'timeout_decorator'
    ],
    entry_points={
        'console_scripts': [
            'hive2elastic_post=post.indexer:main'
        ]
    })
