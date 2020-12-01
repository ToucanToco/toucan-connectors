from pathlib import Path

from setuptools import find_packages, setup

auth_deps = ['oauthlib==3.1.0', 'requests_oauthlib==1.3.0']
bearer_deps = ['bearer==3.1.0']

extras_require = {
    'adobe': ['adobe_analytics==1.2.3'],
    'aircall': bearer_deps,
    'azure_mssql': ['pyodbc==4.0.28'],
    'dataiku': ['dataiku-api-client==8.0.0'],
    'elasticsearch': ['elasticsearch==6.8.1'],
    'facebook': ['facebook-sdk==3.1.0'],
    'github': ['python_graphql_client==0.4.0'],
    'google_analytics': ['google-api-python-client==1.7.5', 'oauth2client'],
    'google_big_query': ['pandas_gbq==0.14.1'],
    'google_cloud_mysql': ['PyMySQL==0.10.0'],
    'google_my_business': ['google-api-python-client==1.7.5'],
    'google_sheets': bearer_deps,
    'google_spreadsheet': ['gspread==3.6.0', 'oauth2client==4.1.3'],
    'hive': ['pyhive==0.6.3'],
    'http_api': auth_deps + ['xmltodict==0.12.0'],
    'lightspeed': bearer_deps,
    'mongo': ['pymongo==3.11.1'],
    'mssql': ['pyodbc==4.0.28'],
    'mysql': ['PyMySQL==0.10.1'],
    'odata': auth_deps + ['tctc_odata==0.3'],
    'odbc': ['pyodbc==4.0.28'],
    'oracle_sql': ['cx_Oracle==8.0.1'],
    'postgres': ['psycopg2==2.8.6'],
    'ROK': ['requests==2.21.0', 'pyjwt==1.7.1', 'simplejson==3.17.2'],
    'sap_hana': ['pyhdb==0.3.4'],
    'snowflake': ['snowflake-connector-python==2.0.4'],
    'toucan_toco': ['toucan_client==1.0.1'],
}
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

install_requires = [
    'authlib==0.15.2',
    'aiohttp==3.6.2',
    'asn1crypto==1.4.0',
    'attrs==20.3.0',
    'cached_property==1.5.2',
    'cryptography==3.2.0',
    'chardet==3.0.4',
    'jinja2==2.11.2',
    'cython==0.29.1',
    'future==0.18.2',
    'google-auth==1.23.0',
    'google-cloud-core==1.4.3',
    'google-cloud-bigquery-storage==2.1.0',
    'google-resumable-media==1.0.0',
    'libcst==0.3.15',
    'numpy==1.19.3',
    'pandas==1.0.4',
    'protobuf==3.13.0',
    'pyasn1==0.4.6',
    'pyasn1_modules==0.2.8',
    'pycparser==2.20',
    'pydantic==1.7.3',
    'pydata-google-auth==1.0.0',
    'pyjq==2.3.1',
    'proto_plus==1.11.0',
    'requests==2.21.0',
    'tenacity==6.2.0',
    'toucan_data_sdk==7.2.0',
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
    version='0.44.2',
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
