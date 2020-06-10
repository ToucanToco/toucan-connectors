from pathlib import Path

from setuptools import find_packages, setup

auth_deps = ['oauthlib', 'requests_oauthlib']
bearer_deps = ['bearer']

extras_require = {
    'adobe': ['adobe_analytics'],
    'aircall': bearer_deps,
    'azure_mssql': ['pyodbc'],
    'dataiku': ['dataiku-api-client'],
    'elasticsearch': ['elasticsearch'],
    'facebook': ['facebook-sdk'],
    'google_analytics': ['google-api-python-client', 'oauth2client'],
    'google_big_query': ['pandas_gbq'],
    'google_cloud_mysql': ['PyMySQL>=0.8.0'],
    'google_my_business': ['google-api-python-client>=1.7.5'],
    'google_sheets': bearer_deps,
    'google_spreadsheet': ['gspread>=3', 'oauth2client'],
    'hive': ['pyhive[hive]'],
    'http_api': auth_deps,
    'lightspeed': bearer_deps,
    'mongo': ['pymongo>=3.6.1'],
    'mssql': ['pyodbc'],
    'mysql': ['PyMySQL>=0.8.0'],
    'odata': auth_deps + ['tctc_odata'],
    'oracle_sql': ['cx_Oracle>=6.2.1'],
    'postgres': ['psycopg2>=2.7.4'],
    'ROK': ['requests'],
    'sap_hana': ['pyhdb>=0.3.4'],
    'snowflake': ['snowflake-connector-python'],
    'toucan_toco': ['toucan_client'],
}
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

install_requires = [
    'aiohttp',
    'cached_property',
    'jinja2',
    'pydantic',
    'pyjq',
    'requests',
    'tenacity',
    'toucan_data_sdk',
    'urllib3==1.24.3',
    'typing-extensions; python_version < "3.8"',
]

classifiers = [
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
]

HERE = Path(__file__).resolve().parent


def get_static_file_paths():
    pkg = HERE / 'toucan_connectors'
    paths = pkg.glob('**/*')
    paths = [str(path.relative_to(pkg)) for path in paths]
    return paths


setup(
    name='toucan_connectors',
    version='0.37.0',
    description='Toucan Toco Connectors',
    long_description=(HERE / 'README.md').read_text(encoding='utf-8'),
    long_description_content_type='text/markdown',
    author='Toucan Toco',
    author_email='dev@toucantoco.com',
    url='https://github.com/ToucanToco/toucan-connectors',
    license='BSD',
    classifiers=classifiers,
    packages=find_packages(include=['toucan_connectors', 'toucan_connectors.*']),
    install_requires=install_requires,
    extras_require=extras_require,
    package_data={'toucan_connectors': get_static_file_paths()},
    zip_safe=False,
)
