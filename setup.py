"""
Setup file for TushareDownloader

Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""


from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="TushareDownloader",
    version="1.0.0",
    author="Yanzhong Huang",
    author_email="bagelquant@gmail.com",
    description="""
    A package for download China A market stock data using tushare api, and automatically store to a local database
    """,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Yanzhong-Hub/TushareDownloader",
    packages=find_packages(),
    python_requires=">=3.10",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],

    extras_require={
        "dev": [
            "pytest>=7.0",
            "twine>=4.0.2",]
    },
    install_requires=[requirements],
)
