from setuptools import setup, find_packages

from rbm import __version__ as version

name = "rbm"

setup(
    name = name,
    version = version,
    author = "Florian Hines",
    author_email = "syn@ronin.io",
    license = "Apache License, (2.0)",
    keywords = "openstack swift middleware",
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        ],
    install_requires=[],
    entry_points={
        'paste.filter_factory': [
            'rbm=rbm.middleware:filter_factory',
            ],
        },
    )
