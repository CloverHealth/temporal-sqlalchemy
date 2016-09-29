from setuptools import setup, find_packages

setup(
    name='temporal-sqlalchemy',
    version='0.1.0',
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
