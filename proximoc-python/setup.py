# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    name='proximo',
    version='0.0.1',
    author='Michal Bock',
    author_email='michal.bock@gmail.com',
    packages=['proximo'],
    url='https://github.com/uw-labs/proximo/tree/master/proximoc-python',
    license='GNU',
    description='Python client for proximo streaming proxy.',
    long_description=open('README.md').read(),
    zip_safe=False,
    include_package_data=True,
    package_data={'': ['README.md']},
    install_requires=['grpcio>=1.18.0'],
    extras_require={
        'development':  ['grpcio-tools>=1.18.0'],
    },
    classifiers=[
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
