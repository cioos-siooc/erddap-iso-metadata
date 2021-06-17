import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="erddap-iso-metadata",
    version="1.0.1",
    author="Scott Bruce",
    author_email="scott.bruce@mi.mun.ca",
    description="A package to generate ISO metadata XML from an ERDDAP dataset metadata",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cioos-siooc/erddap-iso-metadata",
    project_urls={
        "Bug Tracker": "https://github.com/cioos-siooc/erddap-iso-metadata/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: CC-BY 4.0",
        "Operating System :: OS Independent",
    ],
    # package_dir={"": "src"},
    # packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
        'beautifulsoup4>=4.7.1',
        'erddapy>=0.5.0',
        'numpy>=1.16.4',
        'pandas>=0.25.0',
        'Pydap>=3.2.2',
        'pytz>=2019.1',
        'PyYAML==5.1.2',
        'pyyaml-include>=1.1.1.1',
        'requests>=2.22.0',
        'urllib3>=1.25.3',
        'metadata-xml @ git+git://github.com/cioos-siooc/metadata-xml.git',
    ]
)