"""Setup script for temporal-sqlalchemy"""

import sys
import setuptools


SETUP_DEPENDENCIES = []
if {'pytest', 'test', 'ptr'}.intersection(sys.argv):
    SETUP_DEPENDENCIES.append('pytest-runner')

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Intended Audience :: Financial and Insurance Industry',
    'Intended Audience :: Healthcare Industry',
    'License :: OSI Approved :: BSD License',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 3 :: Only',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
    'Topic :: Software Development',
    'Topic :: Software Development :: Libraries',
    'Topic :: Software Development :: Libraries :: Python Modules',
]


setuptools.setup(
    name='temporal-sqlalchemy',
    version='0.4.2',
    description='Temporal Extensions for SQLAlchemy ORM',
    long_description='file: README.rst',
    author='Clover Health Engineering',
    author_email='engineering@cloverhealth.com',
    url='https://github.com/cloverhealth/temporal-sqlalchemy',
    packages=setuptools.find_packages(exclude=('docs*', 'tests*')),
    license='BSD',
    platforms=['any'],
    keywords='sqlalchemy postgresql orm temporal',
    classifiers=CLASSIFIERS,
    python_requires='>=3.4',
    install_requires=[
        'psycopg2>=2.6.2',
        'sqlalchemy>=1.0.15',
        'typing>=3.5.2,<4.0.0;python_version<"3.5"'
    ],
    setup_requires=SETUP_DEPENDENCIES,
    tests_require=[
        'bumpversion>=0.5.0,<1.0.0',    # More for dev_requires
        'tox>=2.0,<3.0.0',
        'tox-pyenv>=1.0,<2.0.0',
    ],
)
