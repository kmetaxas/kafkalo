#!/usr/bin/env python

"""The setup script."""

from setuptools import (
    setup,
    find_packages,
)


with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = [
    "Click>=7.0",
    "requests>=2.0",
    "pyyaml",
    "environs",
    "jinja2",
    # Note that the bundled librdkafka does not include GSSAPI support
    "confluent-kafka[schema-registry,avro,json,protobuf]",
]

setup_requirements = [
    "pytest-runner",
]

test_requirements = [
    "pytest>=3",
]

setup(
    author="Konstantinos Metaxas",
    author_email="kmetaxas@gmail.com",
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="Manage Kafka topics, schemas and permissions",
    entry_points={
        "console_scripts": [
            "kafkalo=kafkalo.cli:main",
        ],
    },
    install_requires=requirements,
    license="GNU General Public License v3",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="kafkalo",
    name="kafkalo",
    packages=find_packages(include=["kafkalo", "kafkalo.*"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/kmetaxas/kafkalo",
    version="0.1.0",
    zip_safe=False,
)
