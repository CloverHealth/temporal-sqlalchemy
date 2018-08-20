from setuptools import setup
import sys

# Check the Python version manually because pip < 9.0 doesn't check it for us.
if sys.version_info < (3, 4):
    raise RuntimeError('Unsupported version of Python: ' + sys.version)

setup(
    setup_requires=['pbr'],
    install_requires=[
        'psycopg2>=2.6.2',
        'singledispatch>=3.4.0.0;python_version<"3.4"',
        'sqlalchemy>=1.0.15',
        'typing>=3.5.2,<4.0.0;python_version<"3.5"'
    ],
    pbr=True,
)
