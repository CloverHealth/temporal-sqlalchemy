"""Setup script for temporal-sqlalchemy"""

import sys
from setuptools import setup, find_packages

if sys.version_info < (3, 5):
    raise Exception('Python version < 3.5 are not supported.')

# Get version information without importing the package
__version__ = None
exec(open('temporal-sqlalchemy/version.py', 'r').read())


setup(
    name='temporal-sqlalchemy',
    version=__version__,
    description='Temporal Decorator for Sqlalchemy ORM',
    url='https://github.com/cloverhealth/temporal-sqlalchemy',
    author='Clover Health Engineering',
    author_email='engineering@cloverhealth.com',
    license='BSD',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='sqlalchemy postgresql orm temporal',
    packages=find_packages(exclude=['docs', 'tests']),

    install_requires=['sqlalchemy', 'psycopg2'],
)
