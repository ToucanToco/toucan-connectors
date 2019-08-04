from setuptools import setup, find_packages

auth_deps = ['oauthlib', 'requests_oauthlib']
extras_require = {
    'adobe': ['adobe_analytics'],
    'azure_mssql': ['pyodbc'],
    'dataiku': ['dataiku-api-client'],
    'elasticsearch': ['elasticsearch'],
    'facebook': ['facebook-sdk'],
    'google_analytics': ['google-api-python-client', 'oauth2client'],
    'google_big_query': ['pandas_gbq'],
    'google_cloud_mysql': ['PyMySQL>=0.8.0'],
    'google_my_business': ['google-api-python-client>=1.7.5'],
    'google_spreadsheet': ['gspread>=3', 'oauth2client'],
    'hive': ['pyhive[hive]'],
    'http_api': auth_deps,
    'mongo': ['pymongo>=3.6.1'],
    'mssql': ['pymssql>=2.1.3'],
    'mysql': ['PyMySQL>=0.8.0'],
    'odata': auth_deps + ['tctc_odata'],
    'oracle_sql': ['cx_Oracle>=6.2.1'],
    'postgres': ['psycopg2>=2.7.4'],
    'sap_hana': ['pyhdb>=0.3.4'],
    'snowflake': ['snowflake-connector-python'],
    'toucan_toco': ['toucan_client']
}
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

install_requires = [
    'aiohttp',
    'jq',
    'jinja2',
    'pydantic',
    'requests',
    'tenacity',
    'toucan_data_sdk'
]

classifiers = [
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Programming Language :: Python :: 3.6'
]

setup(name='toucan_connectors',
      version='0.23.1',
      description='Toucan Toco Connectors',
      author='Toucan Toco',
      author_email='dev@toucantoco.com',
      url='https://github.com/ToucanToco/toucan-connectors',
      license='BSD',
      classifiers=classifiers,
      packages=find_packages(),
      install_requires=install_requires,
      extras_require=extras_require,
      include_package_data=True)
