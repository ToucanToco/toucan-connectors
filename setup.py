from pathlib import Path

from setuptools import find_packages, setup

auth_deps = ['oauthlib==3.1.0', 'requests_oauthlib==1.3.0']
bearer_deps = ['bearer==3.1.0']

extras_require = {
    'adobe': ['adobe_analytics==1.2.3'],
    'aircall': bearer_deps,
    'azure_mssql': ['pyodbc==4.0.30'],
    'clickhouse': ['clickhouse_driver==0.2.0'],  #
    'dataiku': ['dataiku-api-client==9.0.0'],
    'elasticsearch': ['elasticsearch==7.12.0'],
    'facebook': ['facebook-sdk==3.1.0'],
    'github': ['python_graphql_client==0.4.3'],  #
    'google_analytics': ['google-api-python-client==2.1.0', 'oauth2client==4.1.3'],
    'google_adwords': ['googleads==27.0.0'],
    'google_big_query': ['pandas_gbq==0.15.0'],
    'google_cloud_mysql': ['PyMySQL==1.0.2'],
    'google_my_business': ['google-api-python-client==2.1.0'],
    'google_sheets': bearer_deps,
    'google_spreadsheet': ['gspread==3.7.0', 'oauth2client==4.1.3'],
    'hive': ['pyhive[hive]==0.6.3'],
    'http_api': auth_deps + ['xmltodict==0.12.0'],  #
    'lightspeed': bearer_deps,
    'mongo': ['pymongo==3.11.3'],
    'mssql': ['pyodbc==4.0.30'],
    'mysql': ['PyMySQL==1.0.2'],
    'odata': auth_deps + ['tctc_odata==0.3'],
    'odbc': ['pyodbc==4.0.30'],
    'oracle_sql': ['cx_Oracle==8.1.0'],
    'postgres': ['psycopg2==2.8.6'],  #
    'ROK': ['requests==2.21.0', 'pyjwt==2.0.1', 'simplejson==3.17.2'],
    'sap_hana': ['pyhdb==0.3.4'],
    'soap': ['zeep==4.0.0', 'lxml==4.2.5'],
    'snowflake': ['snowflake-connector-python==2.4.1'],
    'toucan_toco': ['toucan_client==1.0.1'],
}
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

install_requires = [
    'authlib==0.15.3',  #
    'aiohttp==3.6.3',
    'cached_property',
    'jinja2==2.11.3',
    'pydantic==1.8.1',
    'pyjq==2.5.2',
    'requests==2.21.0',
    'tenacity==7.0.0',
    'toucan_data_sdk==7.4.2',
    'typing-extensions; python_version < "3.8"',
]

classifiers = [
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
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
    version='0.53.9',
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
