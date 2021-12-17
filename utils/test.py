from toucan_connectors.redshift.redshift_database_connector import (
    RedshiftConnector,
    RedshiftDataSource,
)

# from utils.red_conn_test import (
#     RedshiftConnector,
#     RedshiftDataSource,
# )

p = RedshiftConnector(
    name='local_test',
    host='toucantouco-db.cgfd9jhls7pl.eu-west-1.redshift.amazonaws.com',
    user='toucan',
    password="Toucan+1!",
    cluster_identifier='toucantouco-db',
    port=5439,
    connect_timeout=5,
    authentication_method="db_credentials",
)

# p = RedshiftConnector(
#     iam=True,
#     database='dev',
#     db_user='',
#     user='',
#     password='',
#     cluster_identifier='toucantouco-db',
#     connect_timeout=5,
#     authentication_method="aws_credentials",
# )

# p = RedshiftConnector(
#     iam=True,
#     database='dev',
#     db_user='',
#     user='',
#     password='',
#     cluster_identifier='toucantouco-db',
#     connect_timeout=5,
#     authentication_method="aws_profile",
# )

ds = RedshiftDataSource(
    database='dev',
    domain="test",
    name="redshift",
    query="SELECT * FROM public.sales WHERE qtysold = 4 LIMIT 10",
)
current_config = {'database': ['dev']}


# result = p.get_df(data_source=ds)
result = ds.get_form(p, current_config)
# result = p.check_requirements({'authentication_method': 'db_credentials'})
# result = p.get_redshift_connection_manager()
# result = p._get_connection_params(database=ds.database)
# result = p._build_connection(datasource=ds)
# result = p._get_connection(datasource=ds)
# result = p._start_timer_alive()
# result = p._set_alive_done()
# result = p._get_cursor(datasource=ds)
# result = p._retrieve_tables(datasource=ds)
# result = p._retrieve_data(datasource=ds)
# result = p.get_slice(data_source=ds)
# result = p._get_details(0, True)
# result = p.get_status()

print(f"The result is: {result}")
