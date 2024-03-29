from setuptools import setup 

setup(
    name = 'xrdreporter',
    version = '1.0.2',
    author = 'James Walder',
    author_email = 'james.walder@stfc.ac.uk',
    description = 'Collate, process and report the metrics from the XrootD xrd.report monitoring',
    long_description = 'file: README.md',
    long_description_content_type = 'text/markdown',
    url = 'https://github.com/snafus/xrdreporter.git',
    project_urls ={'Bug Tracker':'https://github.com/snafus/xrdreporter/issues'},
    classifiers =['Programming Language :: Python :: 3','License :: OSI Approved :: MIT License'],
    packages = ['xrdreporter'],
    python_requires = '>=3.6',
    install_requires = ['influxdb', 'influxdb-client'],
    scripts = ['xrdreporter/xrdrep.py',
               'xrdreporter/check.py'],
)
