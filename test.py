from toucan_connectors.redshift.redshift_database_connector import RedshiftConnector, RedshiftDataSource,AuthenticationMethod
p = RedshiftConnector(
    authentication_method=AuthenticationMethod.DB_CREDENTIALS.value,
    name='local_test',
    host='toucantouco-db.cgfd9jhls7pl.eu-west-1.redshift.amazonaws.com',
    port=5439,
    cluster_identifier='toucantouco-db',
    connect_timeout=15,
    user='toucan',
    password="Toucan+1!",
)

# p = RedshiftConnector(
#     authentication_method=AuthenticationMethod.AWS_CREDENTIALS,
#     name='local_test',
#     host='toucantouco-db.cgfd9jhls7pl.eu-west-1.redshift.amazonaws.com',
#     port=5439,
#     connect_timeout=5,
#     cluster_identifier='toucantouco-db',
#     access_key_id='XXX',
#     secret_access_key='XXX',
#     session_token='XXX',
#     region='eu-west-1',
#     db_user='toucan'
# )

# p = RedshiftConnector(
#     authentication_method=AuthenticationMethod.AWS_PROFILE,
#     name='local_test',
#     host='toucantouco-db.cgfd9jhls7pl.eu-west-1.redshift.amazonaws.com',
#     port=5439,
#     connect_timeout=5,
#     cluster_identifier='toucantouco-db',
#     db_user='toucan',
#     profile='toucan',
# )

ds = RedshiftDataSource(database='dev', domain="test", name="redshift", query="SELECT * FROM sales WHERE qtysold = 4")
print(p.get_status())

retrieve_data = p.get_slice(data_source=ds, get_row_count=False)
# retrieve_data = p.get_df(data_source=ds)
print(retrieve_data)