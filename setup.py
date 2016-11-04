"""Setup script for temporal-sqlalchemy"""

import sys
import setuptools

if sys.version_info < (3, 5):
    raise Exception('Python version < 3.5 are not supported.')

# Get version information without importing the package
__version__ = None
exec(open('temporal_sqlalchemy/version.py', 'r').read())

SHORT_DESCRIPTION = 'Temporal Extensions for SQLAlchemy ORM'
LONG_DESCRIPTION = open('README.rst', 'r').read()

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Intended Audience :: Financial and Insurance Industry',
    'Intended Audience :: Healthcare Industry',
    'License :: OSI Approved :: BSD License',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
    'Topic :: Software Development',
    'Topic :: Software Development :: Libraries',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

DEPENDENCIES = [l.strip() for l in open('requirements.txt', 'r')]
TEST_DEPENDENCIES = [l.strip() for l in open('test-requirements.txt', 'r')]
SETUP_DEPENDENCIES = []
if {'pytest', 'test', 'ptr'}.intersection(sys.argv):
    SETUP_DEPENDENCIES.append('pytest-runner')


setuptools.setup(
    name='temporal-sqlalchemy',
    version=__version__,
    description=SHORT_DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author='Clover Health Engineering',
    author_email='engineering@cloverhealth.com',
    url='https://github.com/cloverhealth/temporal-sqlalchemy',
    packages=setuptools.find_packages(exclude=('docs*', 'tests*')),
    license='BSD',
    platforms=['any'],
    keywords='sqlalchemy postgresql orm temporal',
    classifiers=CLASSIFIERS,
    install_requires=DEPENDENCIES,
    setup_requires=SETUP_DEPENDENCIES,
    tests_require=TEST_DEPENDENCIES,
)
