from pathlib import Path

from setuptools import find_packages, setup

auth_deps = ['oauthlib==3.2.0', 'requests-oauthlib==1.3.1']
bearer_deps = ['bearer==3.1.0']

extras_require = {
    'adobe': ['adobe_analytics'],
    'aircall': bearer_deps,
    # awswrangler>=2.15.1 requires pyarrow>=7.0, which might be
    # incompatible with other requirements
    'awsathena': ['awswrangler>=2.14,<2.15'],
    'azure_mssql': ['pyodbc>=3'],
    'clickhouse': ['clickhouse_driver'],
    'dataiku': ['dataiku-api-client'],
    'elasticsearch': ['elasticsearch<8'],
    'facebook': ['facebook-sdk'],
    'github': ['python_graphql_client'],
    'google_analytics': ['google-api-python-client', 'oauth2client'],
    'google_adwords': ['googleads'],
    # google_big_query v3 uses Nullable types (https://pandas.pydata.org/docs/user_guide/integer_na.html) which are
    # not compatible with eval. Once the formula will be compatible with these types, we can upgrade to v3
    'google_big_query': ['google-cloud-bigquery[bqstorage,pandas]==2.*'],
    'google_cloud_mysql': ['PyMySQL>=0.8.0'],
    'google_my_business': ['google-api-python-client>=1.7.5'],
    'google_sheets': ['google-api-python-client>=2'],
    'google_spreadsheet': ['gspread>=3', 'oauth2client'],
    'hive': ['pyhive[hive]'],
    'http_api': auth_deps + ['xmltodict'],
    'lightspeed': bearer_deps,
    'mongo': ['pymongo>=3.6.1'],
    'mssql': ['pyodbc>=3'],
    'mssql_TLSv1_0': ['pyodbc>=3'],
    'mysql': ['PyMySQL>=0.8.0'],
    'odata': auth_deps + ['tctc_odata'],
    'odbc': ['pyodbc>=3'],
    'oracle_sql': ['cx_Oracle>=6.2.1'],
    'net_explorer': ['openpyxl>=3.0.9'],
    'postgres': ['psycopg2>=2.7.4'],
    'Redshift': ['redshift_connector', 'lxml==4.6.5'],
    'ROK': ['requests', 'pyjwt', 'simplejson'],
    'sap_hana': ['pyhdb>=0.3.4'],
    'soap': ['zeep', 'lxml==4.6.5'],
    'snowflake': ['snowflake-connector-python>=2.5', 'pyjwt'],
    'toucan_toco': ['toucan_client'],
}
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

install_requires = [
    'authlib',
    'aiohttp>=3.7.4',
    'cached_property',
    'jinja2',
    'jq',
    'pydantic',
    'requests',
    'tenacity',
    'toucan_data_sdk',
    'typing-extensions; python_version < "3.8"',
]

classifiers = [
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
]

HERE = Path(__file__).resolve().parent


def get_static_file_paths():
    pkg = HERE / 'toucan_connectors'
    paths = pkg.glob('**/*')
    paths = [str(path.relative_to(pkg)) for path in paths]
    return paths


setup(
    name='toucan_connectors',
    version='3.5.1.1',
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
