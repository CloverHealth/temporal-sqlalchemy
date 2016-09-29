import sys
from setuptools import setup, find_packages
from setuptools.command.test import test


class Tox(test):
    user_options = [('tox-args=', 'a', "Arguments to pass to tox")]
    
    def initialize_options(self):
        test.initialize_options(self)
        self.tox_args = None
    
    def finalize_options(self):
        test.finalize_options(self)
        self.test_args = []
        self.test_suite = True
    
    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import tox
        import shlex
        args = self.tox_args
        if args:
            args = shlex.split(self.tox_args)
        errno = tox.cmdline(args=args)
        sys.exit(errno)

setup(
    name='temporal-sqlalchemy',
    version='0.1.0',
    description='Temporal Decorator for Sqlalchemy ORM',
    url='https://github.com/cloverhealth/temporal-sqlalchemy',
    author='Clover Health Engineering',
    author_email='engineering@cloverhealth.com',
    license='MIT',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='sqlalchemy postgresql orm temporal',
    packages=find_packages(exclude=['docs', 'tests']),

    install_requires=['sqlalchemy', 'psycopg2'],
    tests_require=['tox'],
    cmdclass={'test': Tox},
)
