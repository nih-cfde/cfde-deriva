
from setuptools import setup
import re
import io

__version__ = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
    io.open('cfde_deriva/__init__.py', encoding='utf_8_sig').read()
    ).group(1)

setup(
    name='cfde_deriva',
    description='CFDE Deriva Integration Utilities',
    version=__version__,
    zip_safe=False, # make it easier to find JSON, TSV, SQL data
    packages=[
        'cfde_deriva',
        'cfde_deriva.configs',
        'cfde_deriva.configs.portal',
        'cfde_deriva.configs.portal_prep',
        'cfde_deriva.configs.registry',
        'cfde_deriva.configs.submission',
    ],
    package_data={
        'cfde_deriva.configs.portal': ['*.json', '*.tsv', '*.sql'],
        'cfde_deriva.configs.portal_prep': ['*.json', '*.tsv', '*.sql'],
        'cfde_deriva.configs.registry': ['*.json', '*.tsv'],
        'cfde_deriva.configs.submission': ['*.json', '*.tsv', '*.sql'],
    },
    scripts=[],
    requires=['deriva'],
    install_requires=[
        'bdbag>=1.6.3',
        'deriva>=1.5.0',
        'frictionless>=4.0.3',
    ],
    maintainer_email='support@misd.isi.edu',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ])
