from setuptools import setup, find_packages
import sys, os

VERSION = '0.1.0'

LONG_DESCRIPTION = open('README.md').read()

setup(name='SmartRPC',
    version=VERSION,
    description="RPC calling python and SQL functions",
    long_description=LONG_DESCRIPTION,
    keywords='',
    author='Andrew A. Bulatov',
    author_email='bulatovandrew@gmail.com',
    url="https://github.com/a-bulatov/SmartRPC",
    license='MIT',
    platforms='*nix',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'pip',
    ],
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Operating System :: POSIX',
    ],
)