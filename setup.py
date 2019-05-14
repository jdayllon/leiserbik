#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=6.0', 'requests', 'loguru', 'arrow', 'aiohttp', 'pypeln', 'fake_useragent', 'bs4', 'ratelimit',
                'scalpl', 'pypeln', 'python-slugify','iteration_utilities']

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author="Juan David Ayllon Burguillo",
    author_email='jdayllon@gmail.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
    description="Social network information extractor",
    entry_points={
        'console_scripts': [
            'leiserbik=leiserbik.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='leiserbik',
    name='leiserbik',
    packages=find_packages(include=['leiserbik']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/jdayllon/leiserbik',
    version='0.1.0',
    zip_safe=False,
)
