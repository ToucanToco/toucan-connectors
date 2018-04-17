from setuptools import setup, find_packages

extras_require = {
    'mongo': ['pymongo>=3.6.1'],
    'mssql': ['pymssql>=2.1.3'],
    'mysql': ['PyMySQL>=0.8.0'],
    'oracle': ['cx_Oracle>=6.2.1'],
    'postgres': ['psycopg2>=2.7.4'],
    'snowflake': ['snowflake-connector-python'],
    'microstrategy': ['requests']
}
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

install_requires = [
    'toucan_data_sdk',
    'pydantic'
]

setup(name='toucan_connectors',
      version='0.0.9',
      description='Toucan Toco Connectors',
      author='Toucan Toco',
      author_email='dev@toucantoco.com',
      url='https://github.com/ToucanToco/toucan-connectors',
      license='BSD',
      packages=find_packages(),
      install_requires=install_requires,
      extras_require=extras_require,
      include_package_data=True)
