* [33m20b60a8[m[33m ([m[1;36mHEAD -> [m[1;32moracle-table-support[m[33m, [m[1;31morigin/oracle-table-support[m[33m)[m test(oracle): test query/table logic (precendence)
* [33m6a65361[m chore(oracle): coherent casing between query and docstring
* [33mde0d41e[m[33m ([m[1;31morigin/master[m[33m, [m[1;31morigin/HEAD[m[33m, [m[1;32mmaster[m[33m)[m chore(bump): upgrade version to 1.2.5 (#471)
* [33m38c8c0f[m feat(google_bigquery_new): new method to retrieve data in bigquery with pandas (#468)
* [33m6f6f1c8[m chore(bump): upgrade version from 1.2.3 to 1.2.4 (#470)
* [33mfa7c613[m fix(duplicate_small_app): update method signature to save secret (#457)
* [33mc4ff37d[m chore(bump): upgrade version from 1.2.2 to 1.2.3 (#469)
* [33m1e288ab[m feat(optimize_connection_manager): check if connection use good database and/or warehouse (#467)
* [33m0740aca[m feat(get_identifier): use method to get an unique identifier (#466)
* [33mc1cdf26[m fix(hubspot): update URL and method to get result + remove legacy mode (#465)
* [33mdc4172a[m[33m ([m[1;33mtag: v1.2.2[m[33m)[m chore(bump): v1.2.2 (#464)
* [33mfce7cbc[m fix(use_database_warehouse): add request to use the database and warehouse (#463)
* [33me5b9d89[m[33m ([m[1;33mtag: v1.2.1[m[33m)[m feat: add describe method & associated objects in connectors (#461)
* [33m117c21e[m Adding new fields to OneDriveDatasource in order to enable connection with SharePoint workbooks (#456)
* [33maf5585f[m fix: add odbc installation in the readme(for linux systems) (#459)
* [33m26e326d[m feat: add `cache_ttl` field on ToucanConnector and ToucanDataSource (#460)
* [33m871c114[m feat: add get_cache_key method for queries caching (#458)
* [33m1f66545[m chore: bump 1.1.1
* [33m9efd618[m fix(getslice): add get_row_count arg
* [33md01f470[m chore(bump): upgrade toucan-connectors version to 1.1.0 (#454)
* [33m96bf621[m feat(query_pool): add new object to store connection (#443)
* [33m4eac448[m chore(bump): upgrade toucan-connectors version (#453)
* [33m80c42a1[m[33m ([m[1;33mtag: v1.0.3a1[m[33m)[m chore(snowflake): adapt get_slice
* [33m2640e1d[m[33m ([m[1;33mtag: v1.0.0a1[m[33m)[m chore(version): bump to 1.0.0a1 (#449)
* [33mfc03038[m feat(metadata): add metadata in DataSlice
*   [33ma1e5fa5[m Merge pull request #436 from CharlesRngrd/oauth_token
[32m|[m[33m\[m  
[32m|[m * [33m1f2ef7b[m Allow integer in expired_at
* [33m|[m   [33m807ba3a[m Merge pull request #442 from ToucanToco/bump-v0.55.6
[34m|[m[35m\[m [33m\[m  
[34m|[m * [33m|[m [33m5f6e566[m chore(bump connectors): 0.55.6
[34m|[m[34m/[m [33m/[m  
* [33m|[m   [33m1497bed[m Merge pull request #439 from ToucanToco/fix/rollback_hana_upgrade
[36m|[m[1;31m\[m [33m\[m  
[36m|[m * [33m|[m [33mb6e16fd[m tests :: docker-compose - update mssql image
[36m|[m * [33m|[m [33med03dd6[m fix(hana) rollback upgrade
* [1;31m|[m [33m|[m   [33mc6abf88[m Merge pull request #441 from ToucanToco/bump-v0.55.5
[1;31m|[m[1;33m\[m [1;31m\[m [33m\[m  
[1;31m|[m [1;33m|[m[1;31m/[m [33m/[m  
[1;31m|[m[1;31m/[m[1;33m|[m [33m|[m   
[1;31m|[m * [33m|[m [33m3bf52f9[m chore(bump connectors): 0.55.5
[1;31m|[m[1;31m/[m [33m/[m  
* [33m|[m [33md5792df[m Azure mssql parametrization (#426)
* [33m|[m [33ma72690e[m fix: handle datetime 0000-00-00 in mysql connector (#437)
* [33m|[m   [33m2c53e87[m Merge pull request #435 from ToucanToco/chore/bump-connectors-version-0.55.3
[33m|[m[1;35m\[m [33m\[m  
[33m|[m [1;35m|[m[33m/[m  
[33m|[m[33m/[m[1;35m|[m   
[33m|[m * [33m6f9400a[m chore(bump connectors): 0.55.3
[33m|[m[33m/[m  
* [33mc7f877b[m Feat/google big query (#433)
* [33md38d410[m One Drive Connector (#428)
* [33m657b70e[m chore(bump connectors): v0.55.2
* [33m7237efb[m sap_hana : use up to date lib (#430)
* [33m2b0f094[m[33m ([m[1;33mtag: v0.55.1[m[33m)[m chore(bump connectors): v0.55.1
* [33m138b590[m test(default pw secretstr): add unit tests for connectors with default passwords
* [33me42b002[m fix: set default values for secret str
* [33mcdcb91b[m Fix/mssql tl sv1 0 (#429)
* [33mdfc1a1f[m fix(mssql): add new connector for old security constraint (#427)
* [33mf78822e[m fix(snowflake logs): lower case keys
* [33m9c84d00[m chore(snowflake oauth2): clean logged benchmark messages
* [33mb63148a[m feat(snowflake oauth2): add logs for benchmarking
*   [33m1ae33ce[m[33m ([m[1;33mtag: v0.54.5[m[33m)[m Merge pull request #420 from ToucanToco/bump-v0.54.5
[1;36m|[m[31m\[m  
[1;36m|[m * [33med6de47[m cors(bump): v0.54.5
[1;36m|[m[1;36m/[m  
*   [33m3e586cb[m Merge pull request #415 from ToucanToco/fix/salesforce
[32m|[m[33m\[m  
[32m|[m * [33m258d98f[m fix(salesforce): rollback for method get_access_token
[32m|[m * [33mee4a3c8[m fix(salesforce_connectors): multiple improvements !! deprecated method get_access_token -> use get_access_data + new method to return OAuth2SecretData with instance_url -> use this instance_url to get data + add logs on all methods
[32m|[m * [33m63dea5c[m Rename salesforce connector to Salesforce Service Cloud
* [33m|[m   [33md19cfcb[m Merge pull request #419 from ToucanToco/bump-v0.54.4
[34m|[m[35m\[m [33m\[m  
[34m|[m * [33m|[m [33mfd9107c[m chrome(bump): v0.54.4
[34m|[m[34m/[m [33m/[m  
* [33m|[m   [33m9af5e6e[m Merge pull request #418 from ToucanToco/fix/dependencies
[36m|[m[1;31m\[m [33m\[m  
[36m|[m * [33m|[m [33mbf8e030[m fix(dependencies): remove fix versions for all dependencies
[36m|[m * [33m|[m [33md93a6e4[m fix(dependencies): remove version for requests
[36m|[m * [33m|[m [33m0793647[m fix(dependencies): reset requests and pymong
* [1;31m|[m [33m|[m [33m58ffc87[m[33m ([m[1;33mtag: v0.54.3[m[33m)[m refactor: restore enum labels and translate them for snowflake connect
* [1;31m|[m [33m|[m [33m8fb568e[m fix(snowflake): restore authentication enum labels
[1;31m|[m[1;31m/[m [33m/[m  
* [33m|[m [33m9d9f133[m[33m ([m[1;33mtag: v0.54.2[m[33m)[m fix(snowflakes): change category to snowflake
* [33m|[m [33med9a19a[m[33m ([m[1;33mtag: v0.54.1[m[33m)[m feat(snowflake): add category field for frontent sections
* [33m|[m [33m2df665a[m test: unit test for _get_warehouses & named args for connect
* [33m|[m [33mf7b61be[m feat(snowflake oauth2): add missing methods & attributes to use frontend features
* [33m|[m [33m7a00ced[m feat(snowflake): use new tokens and secrets forwarded from caller (#409)
[33m|[m[33m/[m  
*   [33m4106d90[m Merge pull request #414 from ToucanToco/chore/bump-connectors-0.53.9
[1;32m|[m[1;33m\[m  
[1;32m|[m * [33m1f6652f[m chore(bump-connectors): upgrade to 0.53.9
[1;32m|[m[1;32m/[m  
*   [33mbeac593[m Merge pull request #413 from ToucanToco/fix/snowflake_form
[1;34m|[m[1;35m\[m  
[1;34m|[m * [33mf5f78b3[m fix(snowflake_form): fix snowflake_oauth2_test
[1;34m|[m * [33m3d09906[m fix(snowflake_form): fix test
[1;34m|[m * [33mbefc478[m fix(snowflake_form): rename field & error
* [1;35m|[m   [33mfad79bc[m Merge pull request #412 from ToucanToco/fix/dependencies
[1;35m|[m[31m\[m [1;35m\[m  
[1;35m|[m [31m|[m[1;35m/[m  
[1;35m|[m[1;35m/[m[31m|[m   
[1;35m|[m * [33mc4705aa[m fix(dependencies): pyjq from 2.5.1 to 2.5.2
[1;35m|[m[1;35m/[m  
* [33mb649184[m chore/bump-connectors-0.53.7
*   [33m8f0127a[m Merge pull request #407 from ToucanToco/fix/hubspot_connector
[32m|[m[33m\[m  
[32m|[m * [33m9bc4ecc[m fix(hubspot): lint
[32m|[m * [33m4032cc5[m fix(hubspot): Add UT on JsonWrapper
[32m|[m * [33m757014e[m fix(hubspot): Add Class JsonWrapper to replace all import json by this import -> force separator for dump & dumps
* [33m|[m [33m86657fe[m chores(version): bump version to 0.53.7
* [33m|[m [33m87584e6[m fix(snowflake): get_slice now correctly returns a Dataslice
* [33m|[m [33m6363938[m fix(snowflake): get_slice now correctly returns a Dataslice
* [33m|[m [33meee37bc[m[33m ([m[1;33mtag: v0.53.6[m[33m)[m refactor(snowflake oauth2): update some fields properties & default values
* [33m|[m [33mc400f16[m test(snowflakeoauth2): fix unit tests
* [33m|[m [33m0afd272[m fix(snowflake oauth2): default value for client secret
* [33m|[m [33m5cfa0a1[m fix(snowflake oauth): hide non user defined fields
[33m|[m[33m/[m  
*   [33mcdf45d2[m Merge pull request #405 from ToucanToco/clean/clean
[34m|[m[35m\[m  
[34m|[m *   [33m88ca0df[m Merge branch 'master' into clean/clean
[34m|[m [36m|[m[34m\[m  
[34m|[m [36m|[m[34m/[m  
[34m|[m[34m/[m[36m|[m   
* [36m|[m   [33m1848d69[m Merge pull request #404 from ToucanToco/bump-v0.53.3
[1;32m|[m[1;33m\[m [36m\[m  
[1;32m|[m * [36m|[m [33m1d7dcba[m chore(version): bump-v0.53.3
[1;32m|[m[1;32m/[m [36m/[m  
* [36m|[m   [33m8da8c8a[m Merge pull request #403 from ToucanToco/bump-v0.53.2
[1;34m|[m[1;35m\[m [36m\[m  
[1;34m|[m * [36m|[m [33m793404b[m chore(version): bump-v0.53.2
[1;34m|[m[1;34m/[m [36m/[m  
* [36m|[m [33m24df375[m[33m ([m[1;33mtag: v0.53.1[m[33m)[m feat(snowflake get_slice): bump version
* [36m|[m [33ma5ec851[m feat(snowflake get_slice): remove the now useless `max_rows` from SnowflakeDataSource
* [36m|[m [33mc91e243[m feat(snowflake get_slice): add test for offset
* [36m|[m [33m4569e60[m feat(snowflake get_slice): optimized get_slice for snowflake
[1;35m|[m * [33md43b9b7[m chrome(bump): v0.53.5
[1;35m|[m[1;35m/[m  
*   [33m436b287[m Merge pull request #399 from ToucanToco/fix/wootric_bulk
[1;36m|[m[31m\[m  
[1;36m|[m * [33m5378a85[m fix(wootric_bulk): error on fetch data - number of pages fetch are not number of pages asked
* [31m|[m   [33m5c5f372[m Merge pull request #387 from ToucanToco/feat/snowflake_form
[31m|[m[33m\[m [31m\[m  
[31m|[m [33m|[m[31m/[m  
[31m|[m[31m/[m[33m|[m   
[31m|[m *   [33m5ae127c[m feat(snowflake_form): merge master
[31m|[m [34m|[m[31m\[m  
[31m|[m [34m|[m[31m/[m  
[31m|[m[31m/[m[34m|[m   
* [34m|[m [33m6213b6c[m feat(snowflake): SnowFlakeDatasource now has a 'max_rows property (#394)
* [34m|[m [33mdd37891[m[33m ([m[1;33mtag: v0.52.1[m[33m)[m chores(versions): bump connectors version 0.52.1
* [34m|[m [33m1929701[m fix: add github connector _oauth_trigger attribute
* [34m|[m [33md0886fe[m fix(oauth2): re-add the function is_oauth2_connector
* [34m|[m [33m28e969d[m[33m ([m[1;33mtag: v0.52.0[m[33m)[m chores: bump connectors version to 0.52.0
* [34m|[m [33m823c20e[m refactor: updated is_xxx_connector and added attributes to all oauth2 connectors
* [34m|[m [33me502c07[m test: unit tests for oauth2 connector check functions
* [34m|[m [33m3c1639e[m refactor: updated oauth2 class check
* [34m|[m [33medf688e[m tests(snowflake oauth2): unit tests for snowflake oauth2 connector
* [34m|[m [33m8ee3362[m feat: snowflake oauth2 connector
* [34m|[m [33m6609866[m feat: SnowflakeOAuth2 connector
[35m|[m * [33ma5c0eb4[m add dataiku import
[35m|[m *   [33m6c8f984[m Merge branch 'master' into feat/snowflake_form
[35m|[m [36m|[m[35m\[m  
[35m|[m [36m|[m[35m/[m  
[35m|[m[35m/[m[36m|[m   
* [36m|[m   [33m4e8e468[m[33m ([m[1;33mtag: v0.51.16[m[33m)[m Merge pull request #392 from ToucanToco/bump-v0.51.16
[1;32m|[m[1;33m\[m [36m\[m  
[1;32m|[m * [36m|[m [33m61bf815[m chore(version): bump-v0.51.16
[1;32m|[m[1;32m/[m [36m/[m  
* [36m|[m   [33m2e65923[m Merge pull request #391 from ToucanToco/fix/wootric
[1;34m|[m[1;35m\[m [36m\[m  
[1;34m|[m * [36m|[m [33m76bdb5f[m fix(wootric): fix typoi
[1;34m|[m * [36m|[m [33ma89cce6[m fix(wootric): update attribute max to replace lte by le
* [1;35m|[m [36m|[m   [33m296641c[m[33m ([m[1;33mtag: v0.51.15[m[33m)[m Merge pull request #390 from ToucanToco/bump-v0.51.15
[1;36m|[m[31m\[m [1;35m\[m [36m\[m  
[1;36m|[m * [1;35m|[m [36m|[m [33m4eafdea[m chore(version): bump-v0.51.15
[1;36m|[m[1;36m/[m [1;35m/[m [36m/[m  
* [1;35m|[m [36m|[m [33m559fb70[m Merge pull request #388 from ToucanToco/fix/wootric
[32m|[m[1;35m\[m[1;35m|[m [36m|[m 
[32m|[m * [36m|[m [33m605210d[m fix(wootric): fix lint
[32m|[m * [36m|[m [33m5563a5c[m fix(wootric): PR return
[32m|[m * [36m|[m [33m63c5a51[m fix(wootric): fix test with new crawl method
[32m|[m * [36m|[m [33m8a6f0ea[m fix(wootric): update quote for lint
[32m|[m * [36m|[m [33m799c263[m fix(wootric): update documentation for batch_size parameter
[32m|[m * [36m|[m [33m16c4d67[m fix(wootric): fix on fetch live data - batch_size parameter not used correctly
[32m|[m[32m/[m [36m/[m  
[32m|[m * [33mdbef4bf[m feat(snowflake_form): Clean form - remove unused field
[32m|[m * [33mcf0fc8a[m feat(snowflake_form): remove test
[32m|[m * [33m917008e[m feat(snowflake_connector): fix pydantic / FormBuilder
[32m|[m * [33m449c1e2[m feat(snowflakeconnector): coverage
[32m|[m * [33md539b67[m feat(snowflakeconnector): resolve issue on RetryPolicy encapsulation
[32m|[m * [33m7c1a669[m feat(snowflakeconnector): force version for requirements
[32m|[m * [33m63d5eb9[m feat(snowflakeconnector): format snowflake connector form
[32m|[m[32m/[m  
* [33mdaa5029[m[33m ([m[1;33mtag: v0.51.14[m[33m)[m fix: typo in gsheet connector (#386)
* [33mf051137[m[33m ([m[1;33mtag: v0.51.13[m[33m)[m chore(version): bump v0.51.13 (#385)
* [33m395db61[m feat(SSO): Make snowflake connector be able to hold external tokens and credentials (#384)
* [33m3c92540[m[33m ([m[1;33mtag: v0.51.12[m[33m)[m feat: add secrets_storage_version attribute
* [33m1b3d5a6[m fix(snowflake): fix snowflake parametrization (#383)
* [33mf56c883[m feat(google sheets2): re-enable test
* [33m24e7756[m feat(google sheets2): skip test that fail due to external ressource being down.
* [33m5994bd8[m feat(google sheets2): fix test
* [33madc0bff[m feat(google sheets2): the google sheets 2 now parse dates that are in the column `data_source.parse_dates`
* [33me57e094[m chore(version): bump-v0.51.10 (#380)
* [33mdbe24a5[m fix(snowflake): improve verbosity of get_status (#376)
* [33me6619a0[m chore: format files after black update
* [33m9644c6d[m chore: Update & Freeze black
* [33mdf81604[m[33m ([m[1;33mtag: v0.51.9[m[33m)[m chore(version): bump v0.51.9 (#375)
* [33m4b21e86[m[33m ([m[1;33mtag: v0.51.8[m[33m)[m feat(snowflake): enable the snowflake connector to test its connectivity (#372)
* [33m8601624[m feat: add connectorsecretsdata model
* [33m792a1af[m fix:skip flaky aircall test
* [33m977a5d4[m refactor(sql connectors): add util pandas_read_sql (#369)
* [33mc8a144f[m fix(nosql apply parameters to query): keep str type when appropriate (#371)
* [33mb8c92ff[m feat(sql connectors): convert basic jinja to printf templating style (#368)
* [33m4d99c02[m[33m ([m[1;33mtag: v0.51.6[m[33m)[m chore(version): bump-v0.51.6
* [33m1059637[m chore(http-api): ordered keys (#364)
* [33md03924f[m[33m ([m[1;33mtag: v0.51.5[m[33m)[m chore(version): bump-v0.51.5
* [33m8b5dea3[m feat(facebook ads): Add insights endpoint
* [33ma1f4277[m[33m ([m[1;33mtag: v0.51.4[m[33m)[m fix(soap connector): serialize response to handle return format
* [33m816b39d[m[33m ([m[1;33mtag: v0.51.3[m[33m)[m chores: bump connectors version to 0.51.3
* [33mda81764[m refactor(http-api connector): title case
* [33me2430d3[m refactor(http-api connector): fix tests
* [33m4ff83d0[m refactor(http-api connector): update titles and descriptions for datasource
* [33m5b7d697[m doc: added a comment about nested responses
* [33md5a0d27[m refactor: moved tests on response list content to helpers
* [33m0b9725a[m refactor: moved tests on response format to helpers
* [33mfd89850[m refactor(soap connector): improved response parsing
* [33m3defa67[m chore: bump version to 0.51.2
* [33m8a51fdd[m facebook ads: fix the data retrieval step
* [33m84b7126[m refactor: remove test_soap dir
* [33m7101580[m[33m ([m[1;33mtag: v0.51.1[m[33m)[m chore: bump version to 0.51.1
* [33mc924878[m snowflake: python snowflake connector version (#357)
* [33mb345f68[m[33m ([m[1;33mtag: v0.51.0[m[33m)[m feat: flatten rendred nested lists in nosql_apply_parameters_to_query (#354)
* [33ma7182ee[m feature(mssql) add support for interpolating server side values (#353)
* [33madfb381[m feat: New Facebook ads connector (#349)
* [33mecd5508[m[33m ([m[1;33mtag: v0.50.0[m[33m, [m[1;33mtag: v0.49.0[m[33m)[m refactor: refactored after review
* [33mef9b12f[m Update toucan_connectors/google_adwords/doc.md
* [33m1ea75ab[m feat: google adwords connector
* [33m3c97b5d[m feat: SOAP Connector
* [33m6f09c48[m fix(snowflake): convert parameters to qmark style (#350)
* [33m43ff625[m[33m ([m[1;33mtag: v0.48.0[m[33m)[m chores(version): bump to 0.48.0
* [33m6ad1715[m Update toucan_connectors/clickhouse/clickhouse_connector.py
* [33md7ed452[m feat: added ssl connection option
* [33m2aaadae[m refactor: reordered data source form fields
* [33m550012d[m feat(clickhouse): added unit tests
* [33m86c629a[m feat: clickhouse connector
* [33mde10e43[m[33m ([m[1;33mtag: v0.47.0[m[33m)[m chore: v0.47.0
* [33ma82131d[m chore: add test
* [33m79ef488[m feat(httpapi): flatten nested column
* [33m017d243[m hubspot: add logo
* [33m0604c41[m fix(snowflake): make refresh mechanism use a templatable user
* [33m2618f8c[m feat(hubspot): Improve Hubspot datasets management  (#340)
* [33m87b724a[m chores: bump v46
* [33mebd05d1[m[33m ([m[1;33mtag: v0.46.0[m[33m)[m refactor: added date format fallback in case of wrong date format
* [33ma07efef[m refactor: refactored after peer review
* [33m9d9ad41[m refactor: replaced filter field by flatten column for column unesting
* [33m1ee8eed[m feat: linkedinads connector
* [33maef35ea[m feat(snowflake): handle snowflake roles for authentication
* [33me71d6cb[m fix(snowflake): Use 'exp' value from access_token
* [33m2f0afcd[m fix(hubspot): improve documentation for Hubspot connector
* [33mfebab6a[m[33m ([m[1;33mtag: v0.45.14[m[33m)[m chore: bump to version 0.45.14 (#339)
* [33m6770d81[m fix(mongo-connector): make regex match case insensitive (#338)
* [33m8355b0f[m feat(hubspot): new connector for Hubspot
* [33m29688a5[m snowflake: add refresh mechanism
* [33mef2e019[m[33m ([m[1;33mtag: v0.45.13[m[33m)[m chores: connectors v0.45.13
* [33mf4c2db4[m refactor: added unit tests for sleep duration
* [33md253922[m feat(aircall): added rate limit handling
* [33macc4c32[m feat(githubconnector): added ratelimit info extraction & handling
* [33m2774831[m Fix: get form should work even if connect fails (#330)
* [33md3fefd9[m[33m ([m[1;33mtag: v0.45.12[m[33m)[m refactor: changed start variable name
* [33mf5c0830[m refactor: limit PR extraction to 1 year in get_pages
* [33m0364cec[m fix: Filtering CLOSED PR and changing PR author definition
* [33mabb74e8[m[33m ([m[1;33mtag: v0.45.10[m[33m)[m chores: bump to 0.45.10
* [33m3f2323e[m fix(snowflake): Close connections after usage and mini fix
* [33mae7a41f[m snowflake: fix how query parameters are applied
* [33m9c7d9b8[m fix(snowflake): freeze pyarrow
* [33m2b90a00[m[33m ([m[1;33mtag: v0.45.9[m[33m)[m chores: toucan connectors bump 0.45.9
* [33me2a668b[m refactor: added unit test for the bug fix
* [33m5cc5a81[m fix:aircall query params
* [33m5be6d16[m[33m ([m[1;33mtag: v0.45.8[m[33m)[m fix(GithubConnector): using a get instead of a key on latest_retrieved_object
* [33m6e1cf8c[m[33m ([m[1;33mtag: v0.45.7[m[33m)[m v0.45.7
* [33m6670deb[m Snowflake: enable warehouse selction
* [33m6052b53[m refactor: fetch_pages can now have a query parameters dict
* [33ma1864a8[m feat: add a provided_token optional field
* [33m8e5ffe9[m snowflake: fix behavior for default authentication
* [33m346e5a1[m feat(Snowflake): enable database selection
* [33mf628025[m snowflake: fix failing test
* [33m9cc284f[m[33m ([m[1;33mtag: v0.45.6[m[33m)[m v0.45.6
* [33me943968[m fix: support regex with non-string values (#316)
* [33mb836eb7[m v0.44.5
* [33ma357f99[m Snowflake: Add Oauth support
* [33m60ccea8[m fix: onboarded suggested changes from peer review
* [33mbced040[m feat: Automatic refresh token in HttpApiConnector
* [33md08b74d[m fix:on-boarded suggested changes from peer review
* [33ma192170[m feat(Github Connector): enable incremental capability
* [33mce8ed06[m feat: handle datetime in nosql_apply_parameters_to_query (#299)
* [33med56f6d[m chores(versions): bump connectors version to 0.45.4
* [33mf169757[m fix: onboarded suggested changes from review, fixed issue on default db
* [33m01833d6[m feat(postgresconnector): introspection in data source form
* [33m19f0c13[m fix: on-boarded suggested changes from review
* [33m08f8497[m feat(Oracle): Introspection in Oracle Data Source
* [33m70066b6[m fix: Onboarded suggested changes
* [33mca50884[m feat(MSSQL): Introspection in MSSQL connectors
* [33m60731c5[m snowflake: Add a default warehouse field
* [33m25d2ad4[m[33m ([m[1;33mtag: v0.45.3[m[33m)[m feat: enable array parameter interpolation in mssql & postgres
* [33m016ffc6[m[33m ([m[1;33mtag: v0.45.2[m[33m)[m fix: changed salesforce logo
* [33m0b1e460[m[33m ([m[1;33mtag: v0.45.1[m[33m)[m fix: Salesforce connector is now in init file
* [33m899e5e2[m feat: New Salesforce Connector
* [33m5adb327[m[33m ([m[1;33mtag: v0.44.14[m[33m)[m v0.44.14
* [33m225f4ed[m fix(mongo): apply match regex at the end of the pipeline (#300)
* [33m48cdf5c[m[33m ([m[1;33mtag: v0.44.13[m[33m)[m chores(version): bump connectors version 0.44.13
* [33mf9aa950[m refactor: onboarded requested changes from review
* [33ma24d762[m fix: convert_to_qmark_paramstyle now handles list parameter
* [33me140c28[m[33m ([m[1;33mtag: v0.44.12[m[33m)[m fix: changed gsheets datasource fields order for better ux
* [33maa824f5[m fix(Gsheets Connectors): Changed domain field title to dataset & fixed parameters tooltip
* [33m92d69a1[m doc: fixed broken links or removed them when needed
* [33m1676c67[m fix(doc): links
*   [33mf590568[m[33m ([m[1;33mtag: v0.44.11[m[33m)[m Merge pull request #294 from ToucanToco/f/bump-v0.44.10
[34m|[m[35m\[m  
[34m|[m * [33m067b8f9[m bump v0.44.11
[34m|[m[34m/[m  
* [33mf699bc3[m refactor(apply_permissions): make function name explicit
* [33md94a0ec[m mssql: support parametrized queries
* [33m04a4fb8[m feat(Snowflake Connector): render database & warehouse from env variables in data source
* [33mb7b9446[m[33m ([m[1;33mtag: v0.44.9[m[33m)[m chore(versions): bump connector version to 0.44.9
* [33m2c26c5c[m fix(MicroStrategy Connector): Set default password if not set
* [33m571d6d9[m[33m ([m[1;33mtag: v0.44.8[m[33m)[m chore(deps): bump connectors v0.44.8
* [33m4ecfd23[m fix(snowflake connector): unpinned snowflake connector version
* [33m2df3f9c[m[33m ([m[1;33mtag: v0.44.7[m[33m)[m chore(versions): bump connectors v0.44.7
* [33m44fff17[m fix(Github): default page_limit=10, sleep btw queries
* [33m23a675e[m[33m ([m[1;33mtag: v0.44.6[m[33m)[m chore(versions): bump connectors v0.44.6
* [33mf2b5da0[m fix(gsheets2): page limit +1
* [33m981f8ca[m chore(versions): bump connectors v0.44.5
* [33m86b6187[m fix(Gsheets_2): get_slice not query only rows_limit rows during preview
* [33m7d60939[m chore(versions): bump connectors v0.44.4
* [33m3643ec1[m feat(Github Connector): new get_organizations method
* [33m678feb6[m feat(Gsheets Connector): implemented get_slice to query only 50 rows of sheets
* [33mf03ce2a[m feat(Github Connector): implemented get_slice to return only 1 page for 3 entities
* [33m496a248[m feat(Aircall Connector): implemented get_slice to retrieve only one page
* [33m5ef5d3f[m[33m ([m[1;33mtag: v0.44.3[m[33m)[m chore(version): bump connectors 0.44.3
* [33me7a227d[m[33m ([m[1;33mtag: v0.44.2[m[33m)[m feat(Github Connector): added page_limit in data source's form
* [33m807dfc9[m fix(CI): added upgrade pip step in CI workflow
*   [33ma8e304d[m Merge pull request #269 from ToucanToco/fix/pin-odbc
[36m|[m[1;31m\[m  
[36m|[m * [33m1468b25[m chore(version): bump version to 0.44.2
[36m|[m * [33mfdc0bac[m chore(deps): pin pyodbc to >=3
[36m|[m[36m/[m  
* [33me3fa75f[m[33m ([m[1;33mtag: v0.44.1[m[33m)[m chore(version):Bump connector version to 0.44.1
* [33md6b098e[m feat(GithubConnector):make organizations available as enum
* [33mcf2c21e[m fix: increased retry limits and decreased sleeping time (#265)
* [33m5cd9c1b[m[33m ([m[1;33mtag: v0.44.0[m[33m)[m chore(version): bump toucan connectors v0.44.0 (#264)
* [33mff9c943[m refactor:On-boarded changes from peer review and added doc
* [33m5df2f07[m feat:Organization name is retrieved from API by default
* [33m8e988f4[m feat:connector now handle retries due to errors or rate limitation
* [33md401f91[m refactor:merged functions and made them asynchronous
* [33m64868a8[m refactor:refactored github_helpers to remove code repeats
* [33md49398c[m feat:created Github connector
* [33m1872dd2[m[33m ([m[1;33mtag: v0.43.15[m[33m)[m chore:bump connectors version to 43.15 (#262)
* [33m08d9f47[m refactor:updated gsheet connectors names (#261)
* [33m28771b2[m[33m ([m[1;33mtag: v0.43.14[m[33m)[m chore:bump connectors version to 43.14 (#260)
* [33m07cb1d4[m fix:MicroStrategy connector accepts empty password
* [33m2059292[m[33m ([m[1;33mtag: v0.43.13[m[33m)[m chores(version): bump version to 0.43.13
* [33m534142d[m fix(googlesheets2 doc): fix the documentation URL
* [33me87e44b[m[33m ([m[1;33mtag: v0.43.12[m[33m)[m chores(version): bump version number
* [33m09ccd89[m chores(oauth2): documentation is now markdown and uses the {{redirect_uri}} placeholder (#255)
* [33m6ead41b[m docs: added help message to oauth connectors config
* [33m82b8b3c[m[33m ([m[1;33mtag: v0.43.11[m[33m)[m bump version to 0.43.11
* [33m32f1e1a[m fixed tests related to redirect_uri
* [33m4616e2d[m the redirect_uri is no longer part of the OAuthConnectorConfig class. it needs to be provided separately as a simple string
* [33m55c2f63[m[33m ([m[1;33mtag: v0.43.10[m[33m)[m chores(version): bump version
* [33md4458ab[m chores(tests): format
* [33m6cdf66c[m refactor(oauth2): renamed connector config -> connector secrets
* [33ma43507f[m fix(oauth2): Added metadata to the OAuth2ConnectorConfig. the redirect_uri has been tagged as 'not user provided'. This means that even the admin SHOULD NOT fill this field. It should be field automatically by a backend. (#233)
* [33m57e1c8f[m enhanced odbc connector query field (#248)
* [33m4bc963d[m[33m ([m[1;33mtag: v0.43.9[m[33m)[m bumps toucan connectors version to 0.43.9 (#246)
* [33mf6af911[m updated test and microstrategy connector to reflect changes from pydantic 1.7 (#247)
* [33m8a8813c[m fixed the odbcinst.ini file (#244)
* [33m9e58c7c[m fixed aiohttp version in setup.py
* [33mbf834bf[m[33m ([m[1;33mtag: v0.43.8[m[33m)[m bumps connectors version to 0.43.8 (#243)
* [33mf257306[m fixed odbc install script (#242)
* [33mc0d1775[m[33m ([m[1;33mtag: v0.43.7[m[33m)[m bumps connectors version to 0.43.7 (#241)
* [33m369c712[m fixed odbc driver install script (#240)
* [33mbe9aabe[m[33m ([m[1;33mtag: v0.43.6[m[33m)[m bumps connectors version to 0.43.6 (#239)
* [33m9a9b7a4[m refactor(oauth2_connector): changed constructor parameter name to auth_flow_id. previous name was misleading. I also fixed the issue that the aircall_connector was giving its name as a workflow_id (#238)
* [33mbfc6fad[m[33m ([m[1;33mtag: v0.43.5[m[33m)[m bumps connectors version to 0.43.5
* [33m47160d2[m fix(test_aircall): optimized imports
* [33m143fcb2[m feat(oauth2): we now passthrough the kwargs to the body of the fetch_tokens request
* [33mb69e065[m tests(aircall): removed a test that is no longer relevant
* [33ma2607e6[m fix(oauth2): we now send in the body the client_id, client_secret
* [33m7dc1578[m[33m ([m[1;33mtag: 0.43.4[m[33m)[m bump connector's version (#235)
* [33mce23b97[m feat(http connector): Added xml support (#232)
* [33m3a5380c[m[33m ([m[1;33mtag: v0.43.3[m[33m)[m added kwargs in build_authorization_url (#234)
* [33m1df39b9[m chores(version): bump version to 0.43.2
* [33mfb8f4fa[m Aircall connector's name changed to Aircall in CONNECTORS_REGISTRY
* [33mc54c51c[m fix(tests/oauth2connector): the client_secret is now stored as a SecretStr. this prevents us from leaking it in logs, and hints at display it always hidden
* [33m4f855d4[m fix(tests/oauth2connector): we now use HttpAPiCOnnector instead of PostgresConnector so we do not have to download its dependance to run tests
* [33mfb68ecc[m fix(tests/oauth2connector): fixed tests in oauth2connector to match the new OAuth2Connector
* [33m5c3e5a3[m fix(aircall): aircall connector is adapted to the changes made in OAuth2Connector.
* [33m33b035e[m tests(connector_config): added test for get_connector_config_form
* [33m5e18a89[m lint(googlesheet2): lint
* [33m9c03709[m fix(GoogleSheets2): bad automatic refactor :(
* [33m265ccfe[m renamed get_form -> get_connector_config_form
* [33md6b706a[m fix(GoogleSheets2): better documentation for the connector configuration
* [33m2cf2408[m chores(oauth2, google_sheets): format & lint
* [33m54fcf02[m added the config class, and a get_form method to connectors that need custom configuration
* [33m970ed80[m bump version number
*   [33m750664b[m[33m ([m[1;33mtag: v0.43.0[m[33m)[m Merge branch 'master' of github.com:ToucanToco/toucan-connectors
[1;32m|[m[1;33m\[m  
[1;32m|[m * [33m608672e[m ROK Connector: Authentication mode with specific JWT (#222)
[1;32m|[m *   [33m6106dcd[m Merge pull request #217 from ToucanToco/f/oauth2-connector
[1;32m|[m [1;34m|[m[1;35m\[m  
[1;32m|[m [1;34m|[m *   [33meb7f75e[m[33m ([m[1;31morigin/f/oauth2-connector[m[33m, [m[1;32mf/oauth2-connector[m[33m)[m Merge branch 'f/oauth2-connector' of github.com:ToucanToco/toucan-connectors into f/oauth2-connector
[1;32m|[m [1;34m|[m [1;36m|[m[31m\[m  
[1;32m|[m [1;34m|[m [1;36m|[m * [33me603ddd[m tests(oauth2): added a test case covering the is_oauth2_connector function
[1;32m|[m [1;34m|[m [1;36m|[m *   [33mc2f8ec8[m Merge branch 'f/oauth2-connector' of github.com:ToucanToco/toucan-connectors into f/oauth2-connector
[1;32m|[m [1;34m|[m [1;36m|[m [32m|[m[33m\[m  
[1;32m|[m [1;34m|[m [1;36m|[m [32m|[m * [33mc58d0f7[m tests(oauth2): added a test case covering the case where we attempt to retrieve token for a non existing auth flow
[1;32m|[m [1;34m|[m [1;36m|[m * [33m|[m [33mea00424[m tests(oauth2): added a test case covering the case where we attempt to retrieve token for a non existing auth flow
[1;32m|[m [1;34m|[m [1;36m|[m [33m|[m[33m/[m  
[1;32m|[m [1;34m|[m [1;36m|[m * [33m847843b[m chores(quickstart): moved aircall quickstart to the quickstart folder
[1;32m|[m [1;34m|[m [1;36m|[m * [33ma91be97[m docs: move quickstart to dedicated folder
[1;32m|[m [1;34m|[m [1;36m|[m * [33m1eaa229[m feat(Aircall_oAuthConnector): New Aircall connector using oAuthConnector (#226)
[1;32m|[m [1;34m|[m [1;36m|[m * [33m6273b6e[m format & lint
[1;32m|[m [1;34m|[m [1;36m|[m *   [33m30ef72b[m Merge branch 'f/oauth2-connector' of github.com:ToucanToco/toucan-connectors into f/oauth2-connector
[1;32m|[m [1;34m|[m [1;36m|[m [34m|[m[35m\[m  
[1;32m|[m [1;34m|[m [1;36m|[m [34m|[m * [33m73c0c7c[m build: attempt to fix sonar coverage exclusions
[1;32m|[m [1;34m|[m [1;36m|[m [34m|[m * [33mfb0629c[m chore(oauth2): move quickstart files into dedicated folder
[1;32m|[m [1;34m|[m [1;36m|[m [34m|[m * [33mfb79996[m chore: update project version in sonar properties
[1;32m|[m [1;34m|[m [1;36m|[m [34m|[m * [33me479ebe[m tests(oauth2): exclude local helpers from coverage
[1;32m|[m [1;34m|[m [1;36m|[m [34m|[m * [33m19956e1[m chore: remove secrets handling via kwargs from ToucanConnectors
[1;32m|[m [1;34m|[m [1;36m|[m [34m|[m * [33m5fb0b92[m tests(google-sheets2): adapt to token retrieval by OAuth2Connector
[1;32m|[m [1;34m|[m [1;36m|[m [34m|[m * [33m91720cf[m fixes for the linter
[1;32m|[m [1;34m|[m [1;36m|[m [34m|[m * [33me1849ac[m feat(oauth2): added the OAuth2Connector, a helper class used to retrieve and store OAuth2 tokens
[1;32m|[m [1;34m|[m [1;36m|[m[1;34m_[m[34m|[m[1;34m/[m  
[1;32m|[m [1;34m|[m[1;34m/[m[1;36m|[m [34m|[m   
[1;32m|[m * [1;36m|[m [34m|[m [33m7a572b0[m Update doc generation script (#219)
[1;32m|[m [35m|[m [1;36m|[m * [33mb53cf4d[m fix(oauth2): we now use a datetime object, and not a timestamp in the token
[1;32m|[m [35m|[m [1;36m|[m * [33md253655[m fix(oauth2): fixed tests.
[1;32m|[m [35m|[m [1;36m|[m * [33m27ce941[m we now raise a AUthFlowNotFound if we cant find the auth_flow_id in the secrets
[1;32m|[m [35m|[m [1;36m|[m * [33m895b384[m fix(oauth2): proper token validation.
[1;32m|[m [35m|[m [1;36m|[m * [33m51254de[m refactor(oauth2): added a function is_oauth2_connector(cls) to know at runtime if a class needs to follow an oauth2 login flow
[1;32m|[m [35m|[m [1;36m|[m * [33m7b7722c[m doc(oauth2) moved quickstart.py from oauth2 to google_sheets_2. added a README with instructions on how to create client_id and client_secret
[1;32m|[m [35m|[m [1;36m|[m * [33m8e105cf[m fixes quickstart.py for oauth2
[1;32m|[m [35m|[m [1;36m|[m * [33m25ce3f6[m you can now add arbitrary data to the state in the oauth2flow.
[1;32m|[m [35m|[m [1;36m|[m * [33m7f6a8da[m fix wrong import
[1;32m|[m [35m|[m [1;36m|[m * [33m319741a[m build: attempt to fix sonar coverage exclusions
[1;32m|[m [35m|[m [1;36m|[m * [33m3fa269d[m chore(oauth2): move quickstart files into dedicated folder
[1;32m|[m [35m|[m [1;36m|[m * [33m605d5fd[m chore: update project version in sonar properties
[1;32m|[m [35m|[m [1;36m|[m * [33me908ce3[m tests(oauth2): exclude local helpers from coverage
[1;32m|[m [35m|[m [1;36m|[m * [33m3d71f6f[m chore: remove secrets handling via kwargs from ToucanConnectors
[1;32m|[m [35m|[m [1;36m|[m * [33m8169b00[m tests(google-sheets2): adapt to token retrieval by OAuth2Connector
[1;32m|[m [35m|[m [1;36m|[m * [33m9e9d654[m fixes for the linter
[1;32m|[m [35m|[m [1;36m|[m * [33m2199e38[m feat(oauth2): added the OAuth2Connector, a helper class used to retrieve and store OAuth2 tokens
[1;32m|[m [35m|[m * [35m|[m [33mdb11adc[m tests(oauth2): added a test case covering the is_oauth2_connector function
[1;32m|[m [35m|[m * [35m|[m [33m5b91337[m tests(oauth2): added a test case covering the case where we attempt to retrieve token for a non existing auth flow
[1;32m|[m [35m|[m * [35m|[m [33md73ce67[m tests(oauth2): added a test case covering the case where we attempt to retrieve token for a non existing auth flow
[1;32m|[m [35m|[m * [35m|[m [33mb894cba[m chores(quickstart): moved aircall quickstart to the quickstart folder
[1;32m|[m [35m|[m * [35m|[m [33m9fad360[m docs: move quickstart to dedicated folder
[1;32m|[m [35m|[m * [35m|[m [33m687d654[m feat(Aircall_oAuthConnector): New Aircall connector using oAuthConnector (#226)
[1;32m|[m [35m|[m * [35m|[m [33m8199d81[m format & lint
[1;32m|[m [35m|[m * [35m|[m [33mcd6fc72[m build: attempt to fix sonar coverage exclusions
[1;32m|[m [35m|[m * [35m|[m [33m30e542f[m chore(oauth2): move quickstart files into dedicated folder
[1;32m|[m [35m|[m * [35m|[m [33m77bb46e[m tests(oauth2): exclude local helpers from coverage
[1;32m|[m [35m|[m * [35m|[m [33m224853a[m tests(google-sheets2): adapt to token retrieval by OAuth2Connector
[1;32m|[m [35m|[m * [35m|[m [33mb8f0b14[m fixes for the linter
[1;32m|[m [35m|[m * [35m|[m [33m1ade5fb[m feat(oauth2): added the OAuth2Connector, a helper class used to retrieve and store OAuth2 tokens
[1;32m|[m [35m|[m * [35m|[m [33m23c4598[m Update doc generation script (#219)
[1;32m|[m [35m|[m * [35m|[m [33md6d6c95[m fix(oauth2): we now use a datetime object, and not a timestamp in the token
[1;32m|[m [35m|[m * [35m|[m [33mcbfc752[m fix(oauth2): fixed tests.
[1;32m|[m [35m|[m * [35m|[m [33m3459d0c[m we now raise a AUthFlowNotFound if we cant find the auth_flow_id in the secrets
[1;32m|[m [35m|[m * [35m|[m [33mf51367d[m fix(oauth2): proper token validation.
[1;32m|[m [35m|[m * [35m|[m [33mae4422b[m refactor(oauth2): added a function is_oauth2_connector(cls) to know at runtime if a class needs to follow an oauth2 login flow
[1;32m|[m [35m|[m * [35m|[m [33m82c14f2[m doc(oauth2) moved quickstart.py from oauth2 to google_sheets_2. added a README with instructions on how to create client_id and client_secret
[1;32m|[m [35m|[m * [35m|[m [33md8d7974[m fixes quickstart.py for oauth2
[1;32m|[m [35m|[m * [35m|[m [33m838daad[m you can now add arbitrary data to the state in the oauth2flow.
[1;32m|[m [35m|[m * [35m|[m [33mbaa55e4[m fix wrong import
[1;32m|[m [35m|[m * [35m|[m [33m4cbb446[m build: attempt to fix sonar coverage exclusions
[1;32m|[m [35m|[m * [35m|[m [33m8bc4d5e[m chore(oauth2): move quickstart files into dedicated folder
[1;32m|[m [35m|[m * [35m|[m [33m3924860[m chore: update project version in sonar properties
[1;32m|[m [35m|[m * [35m|[m [33m7d88dba[m tests(oauth2): exclude local helpers from coverage
[1;32m|[m [35m|[m * [35m|[m [33m699dbe5[m chore: remove secrets handling via kwargs from ToucanConnectors
[1;32m|[m [35m|[m * [35m|[m [33mf706f0c[m tests(google-sheets2): adapt to token retrieval by OAuth2Connector
[1;32m|[m [35m|[m * [35m|[m [33ma3f95b3[m fixes for the linter
[1;32m|[m [35m|[m * [35m|[m [33m932c159[m feat(oauth2): added the OAuth2Connector, a helper class used to retrieve and store OAuth2 tokens
[1;32m|[m [35m|[m[35m/[m [35m/[m  
[1;32m|[m * [35m|[m [33m3c7d1bb[m chore: remove all helpers for python < 3.8
[1;32m|[m * [35m|[m [33m7f16c64[m chore: remove py<3.8 specific code
[1;32m|[m * [35m|[m [33m59cb0da[m build: only support py3.8 from now on
[1;32m|[m * [35m|[m [33m5daa436[m build: reformat all files
[1;32m|[m * [35m|[m [33m8db6147[m build: ad pre-commit tool
[1;32m|[m * [35m|[m [33ma7a2d0d[m build: add black configuration file
* [35m|[m [35m|[m [33mfc96df8[m chores(version): bump version number
[35m|[m[35m/[m [35m/[m  
* [35m|[m   [33mdd13875[m[33m ([m[1;33mtag: v0.42.0[m[33m)[m Merge pull request #220 from ToucanToco/bump-v0.42.0
[36m|[m[1;31m\[m [35m\[m  
[36m|[m * [35m|[m [33me05f5ff[m Update connector version
[36m|[m[36m/[m [35m/[m  
* [35m|[m [33m103c500[m used convert_to_qmark_paramstyle function in odbc connector (#216)
* [35m|[m [33m5dffa65[m build: remove codecov
[35m|[m[35m/[m  
* [33m4c86def[m[33m ([m[1;33mtag: v0.41.4[m[33m, [m[1;31morigin/bump-v0.41.4[m[33m)[m feat(gsheets2): retrieve unformatted values instead of formatted ones (#214)
* [33mb4aa65c[m feat(oauth): made factory kwargsful (#212)
* [33m5eb71e6[m[33m ([m[1;33mtag: v0.41.3[m[33m)[m fix(gsheets2): removed extraneous function (#211)
* [33m540ba58[m[33m ([m[1;33mtag: v0.41.2[m[33m)[m feat(gsheets2): passed secrets as param (#209)
* [33m35b7e72[m[33m ([m[1;33mtag: v0.41.1[m[33m)[m chore: added auth_flow_id to Google Sheets 2 (#207)
* [33m1a84854[m[33m ([m[1;33mtag: v0.41.0[m[33m)[m chore: bumped version to 0.41.0
* [33m4598a07[m test: adapt to ConnectorStatus dataclass
* [33m2cedd13[m chore: move ConnectorStatus to common and test it
* [33ma5e950d[m chore: add convenient method `to_dict` for ConnectorStatus
* [33mb7ee4bc[m chore: use ConnectorStatus dataclass in other connectors
* [33m4c33a96[m feat(gsheets2): simple status check with user email
* [33ma7a7e95[m chore: custom exception type for fetch errors
* [33mf9976e5[m chore(gsheets): rename get_data to authentified fetch
* [33m8649b1b[m[33m ([m[1;33mtag: v0.40.0[m[33m)[m v0.40.0
* [33md6bd71a[m chore: fix typing for secrets
* [33m026b92b[m doc(gsheet2): fix sheet param name
* [33m0fc80b8[m test(gsheets): factor & document pydantic schema differences
* [33mc55b335[m fix(gsheets2): fix fetching of sheets list and test it
* [33md7fd3f2[m chore: remove duplicate function in GSheets connector
* [33m17da288[m feat(googlesheets2): using tokens in requests (#200)
* [33mbd2a4a5[m feat(googlesheets2): fixed url
* [33m8c64b88[m feat(googlesheets2): first pass at passing token (#198)
* [33mc133216[m feat(googlesheets2): added documentation
* [33mde44290[m feat(googlesheets2): added new test for get_form
* [33m2f9f409[m feat(googlesheets2): using tokens in connector
* [33m4cb66ac[m feat(googlesheets2): first pass at passing token (#198)
* [33mc0cd478[m feat(oauth): Modified Google Sheets (#190)
* [33mf5c6174[m Update snowflake_connector.py
* [33maf12bc3[m Update oracle_sql_connector.py
* [33mb20dc25[m Update odata_connector.py
* [33mf6c935f[m Update micro_strategy_connector.py
* [33m26bdc72[m Update google_spreadsheet_connector.py
* [33mdcda90c[m Update google_big_query_connector.py
* [33m87d7958[m Update google_analytics_connector.py
* [33m266019a[m[33m ([m[1;33mtag: v0.39.5[m[33m)[m release: v0.39.5
* [33meb05b5e[m chore: update isort and black goals in Makefile
* [33m6e0e0c3[m feat(gbq): use standard SQL as default query syntax
* [33md240ded[m chore(deps): bump black and re-lint (#199)
* [33mb58c079[m[33m ([m[1;33mtag: v0.39.4[m[33m)[m chore(dep): fixing snowflake CI errors (#197)
* [33m3114884[m chore: give human readable name to project in sonar
* [33mb42add1[m fix(ci): use relative paths in coverage report
* [33m6f84570[m docs: add sonar coverage badge
* [33m1be38ac[m fix(ci): add test files to sonar analysis
* [33m644cd3a[m fix(ci): only execute sonar scan once per commit
* [33m255286e[m Add sonar analysis to the repository
* [33m52ffa92[m[33m ([m[1;33mtag: v0.39.3[m[33m)[m fix: interpolate properly parameters in snowflake
* [33ma7b80e1[m[33m ([m[1;33mtag: v0.39.2[m[33m)[m unpin urllib3 (#195)
* [33m985a981[m[33m ([m[1;33mtag: v0.39.1[m[33m)[m chore(bump): bump patch version to 0.39.1 (#194)
* [33m7280e72[m Snowflake :: Add params support (#193)
* [33m631ba73[m[33m ([m[1;33mtag: v0.39.0[m[33m)[m bump version to 0.39.0
* [33mc82294a[m feat: creates Revinate connector module (#185)
* [33me8d7554[m BigQuery :: fix scopes label
* [33m1e9e619[m fix(mongo): fix status error format
* [33m34b49f4[m[33m ([m[1;33mtag: v0.38.0[m[33m)[m release: v0.38.0
* [33m3632ae4[m fix(test): update expected schemas with pydantic 1.6
* [33m053a7c3[m feat: add info to know if a connector implements `get_status`
* [33m328f194[m release: v0.37.8
* [33me77b920[m install_scripts :: mssql - missing gnupg package
* [33m5986e50[m install_scripts :: databricks - missing unzip package
* [33m1798723[m release: v0.37.7
* [33m6be768f[m install_scripts :: oracle - still missing unzip package
* [33mf773acf[m bump version number
* [33m1ee40c5[m install_scripts :: oracle - add missing wget (#183)
* [33mcb92605[m test: use pytest.mark.parametrize
* [33mc09ab73[m[33m ([m[1;33mtag: v0.37.5[m[33m)[m release: v0.37.5
* [33m6a5ccd5[m fix(elasticsearch): set password properly in client
* [33me016eb9[m chore: do not track mypy cache
* [33mc9b7e21[m[33m ([m[1;33mtag: v0.37.4[m[33m)[m bumping version to 0.37.4
* [33m5c418b3[m fix(error message) :: ETL validation crash in HTTP connector (#179)
* [33m0287823[m bump connector version to 0.37.3
*   [33mdc16896[m Merge pull request #177 from ToucanToco/fix/aircall-connector-gdpr
[1;32m|[m[1;33m\[m  
[1;32m|[m * [33mc66c70a[m Aircall :: do not collect phone numbers of contacts (GDPR sensible...)
[1;32m|[m[1;32m/[m  
* [33mf993bf2[m[33m ([m[1;33mtag: v0.37.2[m[33m)[m bump version to 0.37.2 (#176)
*   [33mf5b301e[m Merge pull request #175 from ToucanToco/fix-schema-elasticsearch
[1;34m|[m[1;35m\[m  
[1;34m|[m * [33m8cb3359[m elasticsearch :: fix password schema
[1;34m|[m[1;34m/[m  
* [33mbad301d[m[33m ([m[1;33mtag: v0.37.1[m[33m)[m update version to v0.37.1 (#174)
* [33m7f93683[m for retrying Aircall API if error (#172)
* [33m336aeab[m[33m ([m[1;33mtag: v0.37.0[m[33m)[m trying a fix
* [33m195c681[m v0.37.0
* [33m7bfad14[m [mongo] more verbose explain infos
*   [33me9d4391[m Merge pull request #170 from ToucanToco/f/trello-param-filter
[1;36m|[m[31m\[m  
[1;36m|[m * [33madd0f3b[m Trello connector implements "filter" param of trello api
[1;36m|[m[1;36m/[m  
* [33m60a0e7f[m[33m ([m[1;33mtag: v0.36.2[m[33m)[m bump version to 0.36.2 (#167)
* [33mea08d39[m Fix ::: HTTP connector ::: no top level domain (#166)
* [33m6c64533[m[33m ([m[1;33mtag: v0.36.1[m[33m)[m updated to patch version 0.36.1 (#165)
* [33ma62d3a1[m [lint] fix flake8 issues
* [33m63e674b[m[33m ([m[1;33mtag: v0.36.0[m[33m)[m v0.36.0 (#161)
* [33m106788a[m Test count limit for mongo connector
* [33md31aca8[m Document max counted rows for Mongo connector
* [33m666cb4a[m Prevent mongo connector to count more than 1M rows
* [33mc23e399[m[33m ([m[1;33mtag: v0.35.0[m[33m)[m bump version to 0.35.0 (#160)
* [33m71318b2[m Fix Aircall requests when user is not defined (#158)
* [33mb0d4383[m[33m ([m[1;33mtag: v0.34.0[m[33m)[m Bumped version of Toucan Connectors (#157)
*   [33m845c869[m Merge pull request #156 from ToucanToco/fix/aircall-connector-calls-datasource
[32m|[m[33m\[m  
[32m|[m * [33m56d8e46[m Aircall: fix "calls" datasource Replace null values with the label 'NO TEAMS' in the column "team" of the "calls" dataset (in coherence with what we do for the "users" dataset).
[32m|[m[32m/[m  
* [33m3d16583[m[33m ([m[1;33mtag: v0.33.0[m[33m)[m Bump to v.0.33.0
* [33mff7b84d[m f/modified air call (#149)
* [33m3d3472a[m Set up cache for docker images in CI
* [33mf3ec727[m Skip Hive tests temporarly
* [33m68ce7d8[m[33m ([m[1;33mtag: v0.32.0[m[33m)[m Bump version to 0.32.0
* [33m41df29e[m Introduce typings for ConditionTranslator
* [33mb23eb50[m Extract ConditionTranslator into its own file
* [33m02a0f91[m Factor as much logic as possible in ConditionTranslator
* [33md0eda45[m Use abstract class for ConditionTranslator
* [33mb8be1ef[m Test all operators of translators
* [33m3c0cf40[m Remove NotImplementedError from coverage report
* [33md79995c[m Adapt mongo connector tests to new condition format
* [33ma6d1f25[m Use classes to translate the permission conditions to Mongo.
* [33mf371de5[m Use classes for ConditionTranslator
* [33md7ac95b[m Use the an enum to switch between operators.
* [33m6033d09[m Import PermissionCondition operators
* [33m3a61294[m Clean up
* [33m121ade4[m Lint and comments.
* [33m3c7e431[m Add and use the pandas translator.
* [33m82a6c95[m Use of the mongo translator.
* [33m5761f19[m Add the filter step to mongo translator.
* [33ma8d8acd[m[33m ([m[1;33mtag: v0.31.0[m[33m)[m v0.31.0
* [33m539b78f[m fix python 3.8 tests
* [33mcfd32da[m switch from pymssql to pyodbc
* [33mf29039e[m Switch to Github actions
* [33m23ded72[m Switch from jq to pyjq
* [33m1db51b6[m ignore pyenv files
* [33m0364584[m[33m ([m[1;33mtag: v30.0.5[m[33m, [m[1;31morigin/cir[m[33m)[m [version] 0.30.5
* [33mc9df63e[m aircall tests are flaky
* [33ma0a1fa6[m [flake8] enforce single quotes
* [33m444c3aa[m mongo :: change implementation of the query facetizing in get_slice()
* [33me3691f7[m[33m ([m[1;33mtag: v.0.30.4[m[33m)[m toucan-connectors :: bump version to 0.30.4
* [33me84e379[m[33m ([m[1;33mtag: v0.30.3[m[33m)[m Oracle:: add Github installation link on the readme
* [33mce0385f[m README :: Umanis feedback
* [33m54b3236[m add Indexima connector
* [33m18763d5[m add Denodo connector
* [33m4c909f5[m add Databricks Delta Lake connector
* [33m9f6e26f[m add AWSRedshift connector
* [33m784bdfa[m add AWSDocumentDB connector
* [33m3668013[m [ci] Fix CI by running on ubuntu 16.04 instead of 14.04
* [33m7421da5[m Fix :: Doc Link python requests
* [33m8eb1f51[m bump version number
* [33m0233b80[m include PNG (and all other static files) on make build
* [33m114a7d8[m[33m ([m[1;33mtag: v0.30.2[m[33m)[m [version] 0.30.2
* [33m8f79254[m [google sheets] set dataframe columns properly
* [33m95393d1[m[33m ([m[1;33mtag: v0.30.1[m[33m)[m [version] 0.30.1
* [33m4cbd34a[m [mongo] do not raise an error if password is set to None
* [33m3da8893[m [version] 0.30.0
* [33meb64153[m [pydantic 1 :: end] remove freeze on `pydantic` version
* [33m582244f[m [pydantic 1] `copy()` now returns a deep copy
* [33m93518a9[m [pydantic 1] fix :: update schemas generated by pydantic in tests
* [33m7f7f819[m [pydantic 1] fix :: pydantic now uses __dict__ for attributes
* [33mf4bc88e[m [pydantic 1] Replace deprecated UrlStr and DSN types
* [33m173445e[m [pydantic 1 :: start] Replace Schema with Field
* [33m70f288d[m [aircall] some routes do not return 'meta' information
* [33m034461d[m [setup] put bearer dependencies in a single variable
* [33m338ebc6[m [aircall] Manage pagination and query parameters
* [33mae1516a[m[33m ([m[1;33mtag: v0.29.1[m[33m)[m [versioning] Bump to v0.29.1
* [33m77398cf[m fix :: bearer dependency is not mandatory
* [33m6752ca7[m[33m ([m[1;33mtag: v0.29.0[m[33m)[m [version] 0.29.0
* [33m54e723d[m [lightspeed] Add new connector
* [33mc325d19[m [aircall] Add new connector
* [33m864f3ff[m create the html src for each connector logo
* [33ma4721fd[m [build] add images of connectors in the package
* [33m0064e8b[m [connector registry] add logos and all connector details in a unique object
* [33md977270[m [google_sheets] Add connector with bearer integration
* [33mb4a8407[m [lint] make format
* [33mb5cf153[m [http] schema :: fix some wrong titles
* [33m1edf534[m Create ROK connector
* [33mb429f12[m Extract jq related schema and function
* [33m014f0b8[m[33m ([m[1;33mtag: v0.28.3[m[33m)[m Bump version : 0.28.3  (fix mongo to atlas connection)
* [33mf536b3d[m Filter out optional parameters from MongoClient init
* [33mb38acb2[m [ci] Change PPA to download python 3.6 on ubuntu 14.04
* [33mc49b2c9[m [schema] improve odata_connector
* [33m17f3b41[m [schema] improve dataiku_connector
* [33me16c1ed[m [schema] improve snowflake_connector
* [33m0fce4a6[m [schema] improve sap_hana_connector
* [33mafc2f36[m [schema] improve azure_mssql_connector
* [33m7cd6580[m [schema] improve oracle_sql_connector
* [33m120417c[m [schema] improve postgresql_connector
* [33me07bf1b[m [schema] improve mssql_connector
* [33m7762683[m [schema] improve google_cloud_mysql_connector
* [33m8572e2b[m [schema] improve google_big_query_connector
* [33m57dd55a[m [schema] improve google_spreadsheet_connector
* [33mc378dee[m [schema] improve google_analytics_connector
* [33maf1e427[m [schema] improve http_api_connector
* [33m51e8bd4[m [schema] improve mongo_connector
* [33m6fa8b30[m [schema] improve micro_strategy_connector
* [33mf66442d[m [schema] improve mysql_connector
* [33m786da44[m [lint] fix all bad quotes
* [33mf1e85c8[m [lint] Add flake8-quotes
* [33m337ed69[m[33m ([m[1;33mtag: v0.28.2[m[33m)[m MANIFEST.IN :: add missing bash scripts
* [33m33257c7[m Add shell install scripts for some connectors (#123)
* [33m80ed03d[m v0.27.2
* [33m1f648f9[m Backport of v0.25.2
* [33mf8c3bb8[m v0.27.1
* [33m609de84[m Fix snowflake use warehouse
* [33mca4b1f7[m v0.27.0
* [33m66d8f42[m dont hardcode field names
* [33m3ca092a[m rem ipdb comment
* [33m5a01773[m fix import
* [33m626caa0[m fix urllib3
* [33m1545082[m Use MongoClient parameters instead of building a URI
* [33m30eb0dd[m on mongo connector, apply permissions in the first $match of the pipeline (#119)
* [33md4c5709[m v0.26.0
* [33m18c7f45[m Test limit parameter in mongo's get_df_with_regex
* [33mfe90b04[m New method of mongo connector: get_df_with_regex
* [33mbf9fb25[m Handle backticks in permissions (#117)
* [33mface2cc[m Repair broken link in Microstrategy connector doc
* [33mb9d9e26[m [pkg] bump version to 0.25.0
* [33m6875d03[m Fix :: validation cache should be proper to a MongoConnector instance
* [33mbe68a15[m Add `isort` options in setup.cfg
* [33ma503fa8[m README :: Add a step for `make format`
* [33m88ff18c[m Update circleci config
* [33mb1f6797[m Add rules to make flake8 compliant with black
* [33m49e5e9a[m make format
* [33m579de63[m Add black and isort and clean the Makefile
* [33m4333b77[m Bump to v0.24.2
* [33mf78ca06[m Fix :: limit can be `None` in `get_slice` method
* [33m516872e[m Bump to v0.24.1
* [33m15f17ec[m Fix :: pagination of get_slice connectors method (#108)
* [33m627e9db[m Bump to v0.24.0
* [33m5c59db8[m Add root __init__ to set PYTHONPATH properly when running pytest directly
* [33m0625ea8[m HttpAPI :: Support SSL Cert verification
* [33m6bcadf4[m MongoConnector :: keep the client alive and cache validate methods
* [33m7d03454[m Fix query to keep type str for int in string (#103)
* [33m2452e10[m Hotfix :: avoid having "'...'" for strings (can not json.loads()) (#102)
* [33m978388e[m Fix :: empty password for Microstrategy + unescape breaklines for GoogleCredentials (#101)
* [33md1a55a0[m Fix :: handle dot syntax for dict in jinja templates (#100)
* [33m1a82d02[m Feature :: get form (#96)
* [33m35c7d63[m microstrat connector :: add ability to target an attribute or a metric using its raw ID (#98)
* [33m6ca50e4[m Mongo :: translate jinja + fix $sort (#97)
* [33m2cd61f3[m Elasticsearch :: Read aggregations in response (#94)
* [33m39e18ba[m  Feature :: Add jinja to render parameters in query and permissions (#93)
* [33m61f78b4[m mongo connector :: fix get_df_and_count when the query uses $group aggregation (#95)
* [33md2a8c58[m Fix :: Fix requirements by connector (#91)
* [33m6e9ca02[m wootric :: new connector (#88)
* [33m8f8d5f2[m Tech :: Make jq required (#90)
* [33mef3b560[m [template] follow recent get_df vs. _retrieve_data API changes
* [33m2900525[m HttpApi :: Add proxies parameter (#87)
* [33mc59b468[m microstrat :: improvements (#86)
* [33mbbfacc6[m Feature :: Apply permissions to datasource (#85)
* [33m1d76ffd[m Hotfix :: OData connector (#84)
* [33m7ce3ea4[m Bump version to 0.17.0
* [33mbe13c44[m Get status and all connectors (#82)
* [33mf6fc4bd[m Implement a retry policy on connectors. (#80)
* [33m743704c[m Feature :: add methods get_df_and_count + explain (#81)
* [33m4eca84a[m new connector :: trello (#78)
* [33m5fee0d5[m Feature :: update mongo connector (#79)
* [33m1112919[m Feature :: move doc to simplify auto generation in docs.toucantoco.com (#77)
* [33m3154e56[m F/facebook insights (#74)
* [33m29aba5e[m  Feature :: add connector elasticsearch (#75)
* [33mcacee42[m GoogleMyBusiness connector (#70)
* [33mb1c3876[m Upgrade pydantic (#68)
* [33m5bc0e64[m add wheel pkg in test virtualenv (for bdist_wheel)
* [33m46dc9cc[m Feature :: Azuremssql with pyodbc (#69)
* [33m732846f[m Bring oracle back (#72)
* [33m6535459[m  Http api : authorize dict for parameter data (#71)
* [33m771c511[m Doc :: update http_api
* [33m6a5010c[m Allow validation field (#67)
*   [33mb94033a[m Merge pull request #63 from ToucanToco/f/remove_magento
[34m|[m[35m\[m  
[34m|[m * [33m7abe0c3[m spelling
[34m|[m * [33m25f1f52[m Doc details
[34m|[m * [33m7df7f94[m Goodbye
* [35m|[m [33m7330079[m Add ssl option for mongo connector (#64)
[35m|[m[35m/[m  
* [33md59b748[m bump version
* [33m36dcc53[m Update big query connector doc for scopes (#62)
* [33m206c7c0[m google big query :: add scope parameter (#61)
* [33m698764a[m fix newline
* [33m3fff298[m hive :: parameters support (#59)
* [33mfea63df[m OData :: improvements (better names for params, more auth) (#56)
* [33m6f08b25[m fix :: apply parameters when the variable is used inside an expression (#58)
* [33m5beafd3[m Hotfix :: Right dependencies for Google connectors (#57)
* [33m1165705[m use tctc_odata from pypi (#55)
* [33ma3e7b4c[m OData connector (#47)
* [33m2cfa629[m Magento :: add connector (#51) (#54)
* [33mf1d9a55[m makefile :: create a new connector from templates (#50)
* [33m5557c5e[m remove 'eval_rst' block (#48)
* [33meda6dec[m http api connector :: Add support for oauth2 "backend application flow" (#43)
* [33m4bc010f[m Feature :: Google Analytics segmentId (#44)
* [33mc79cd80[m Feature :: add skip_rows for google spreadsheet (#42)
* [33m682355d[m Feature :: Google Big Query connector (#41)
* [33mc2909d3[m add available connectors
* [33m8d8a7b4[m Feature :: add live_data option for Datasource (#39)
* [33me1bed03[m Bump patch version following patch confusion
* [33m77270e3[m Hotfix :: httpAPI connector, add support for template and fix json (#40)
* [33m86fec2a[m  Hotfix :: mongo with divide (#38)
* [33ma701ee1[m Postgres :: Add params to data source (#37)
* [33ma7374c4[m Hotfix :: auth on http api connector, not datasource (#36)
* [33mcb56b9c[m fix __init__.py (#35)
* [33ma09dcf2[m hotfix :: AttributeError: 'Method' object has no attribute 'upper'
* [33m1caf515[m hotfix :: validation for http connector
* [33m84bdbc2[m features :: HttpAPI connector (#34)
* [33mb108868[m Update oracle_sql.md (#30)
* [33m8737883[m Feature :: decode bytes columns for MySQL (#32)
* [33mdf4cf40[m Migration to Circle 2.0 (#31)
* [33ma92a935[m Add google credentials doc to ga doc (#29)
* [33m5c19f65[m Feature :: hive connector (#27)
* [33m4a93e3c[m Feature :: support missing parameters in mongo connector (#26)
* [33m3e0a53a[m Feature :: add parameters for Google Analytics (#24)
* [33mfba9f9e[m circleCI :: move to circle 2.0 (#25)
* [33m8d7b285[m new version
* [33m4237335[m Feature :: add parameters to mongo query (#23)
* [33m574ac70[m Feature :: remove type from datasources doc (#22)
*   [33m178b496[m Merge pull request #18 from ToucanToco/f/google_analyics
[36m|[m[1;31m\[m  
[36m|[m *   [33m4dcba75[m Merge branch 'master' into f/google_analyics
[36m|[m [1;32m|[m[36m\[m  
[36m|[m [1;32m|[m[36m/[m  
[36m|[m[36m/[m[1;32m|[m   
* [1;32m|[m [33m4439596[m Feature :: Add a connector for an instance own info (#17)
* [1;32m|[m [33m03a8da6[m Hotfix :: Added default value for dimensions (#21)
[1;33m|[m * [33me6f991a[m Small fixes
[1;33m|[m * [33m29cdcbe[m Better docs
[1;33m|[m *   [33m658a6da[m Merge branch 'f/google_analyics' of github.com:ToucanToco/toucan-connectors into f/google_analyics
[1;33m|[m [1;34m|[m[1;35m\[m  
[1;33m|[m [1;34m|[m *   [33md7b3a59[m Merge branch 'master' into f/google_analyics
[1;33m|[m [1;34m|[m [1;36m|[m[1;33m\[m  
[1;33m|[m [1;34m|[m[1;33m_[m[1;36m|[m[1;33m/[m  
[1;33m|[m[1;33m/[m[1;34m|[m [1;36m|[m   
* [1;34m|[m [1;36m|[m [33m457593d[m MySQL :: parameterized query (#20)
* [1;34m|[m [1;36m|[m [33m2ff978e[m Update google_spreadsheet.md
* [1;34m|[m [1;36m|[m [33me2b8163[m Hotfix :: fix mongo credentials containing special chars (#19)
* [1;34m|[m [1;36m|[m [33m3ce7900[m Generate doc with cson example (#15)
[31m|[m * [1;36m|[m [33m26d7bd9[m Doc
[31m|[m [1;36m|[m[1;36m/[m  
[31m|[m * [33ma03350a[m GA connector
[31m|[m * [33mf45f155[m Initial commit
[31m|[m * [33m8ff011d[m Initial commit
[31m|[m[31m/[m  
* [33mf2ebeb4[m Feature :: adobe analytics (#16)
* [33m210def3[m Fix ::   allow "load" param (ignored) in datasources conf (#14)
*   [33m50983eb[m Merge pull request #13 from ToucanToco/f/add-google-spreadsheet-connector
[32m|[m[33m\[m  
[32m|[m * [33m5c1cd61[m bump version number
[32m|[m * [33mebfa972[m fix tests imports
[32m|[m * [33mc8e45f2[m Documentation
[32m|[m * [33m930db12[m add google spreadsheet connector
[32m|[m[32m/[m  
* [33m0bbf794[m Test details
* [33m166487b[m Bump version to 0.0.13
* [33meec5d83[m Dataiku connector (#12)
* [33me76e1b7[m fix mysql docker
* [33m2bc076f[m bump version
* [33m9a6685f[m Update setup.py
* [33m0ca52ce[m put back __init__ in all directories
* [33m6628e7c[m update tests
* [33mf8e7fe5[m add azure mssql connector (#10)
* [33m5cf19c0[m add sap hana connector (#9)
* [33mb14806a[m add google cloud mysql connector (#8)
* [33m4835aa0[m allow query to end with ; for oracle sql
* [33m285160a[m update oracle sql name
* [33m50a58db[m oracle connector (#7)
* [33me62d530[m lint
* [33m436354d[m typo
* [33m6cd3da5[m fix circle
* [33m730db45[m Update circle.yml
* [33m612d2a5[m Update README.md
* [33m71ddffe[m Update README.md
* [33m95a0380[m Update README.md
* [33m345e555[m new setup
* [33me99a8cf[m Doc :: snowflake (#6)
* [33mb483dae[m Update __init__.py
* [33m24e07a6[m Snowflake connector (#4)
* [33m7078d3c[m Script to generate doc and doc for connectors (#5)
* [33mbc6bb08[m MicroStrategy connector (#3)
* [33m92723ac[m update readme
* [33m03ad4dc[m fix imports
* [33m605c46a[m update readme and imports
* [33m345b0c4[m Make data source and provider schemas explicit (#1)
* [33m50534dc[m upgrade version
* [33mb8eb554[m remove context manager for compatibility
* [33m21e202c[m better
* [33m51b51bb[m bump version
* [33maa4d1e3[m add type to class
* [33m0736b6c[m forgot flake
* [33mbf3b658[m good to go
* [33m572510d[m codecov :: update token
* [33m3e79af7[m rahhh
* [33m4042b6c[m circleci should be ok
* [33me3bf9c2[m fix :: seems like we need setuptools to be upgraded
* [33m0ca5b77[m circleci :: add docker
* [33mfa32bd6[m missing header for sql in circleci
* [33mf7c87da[m install requirements for circleci
* [33m07c4801[m upgrade readme
* [33mbb6dedf[m fix circleci python version
* [33me83a99a[m packaging
* [33m0e996bc[m fix requirements
* [33m39b07a6[m typo
* [33m8135ca4[m add mongo and mssql :: all tests ok
* [33m8b926e3[m abstract class + 3 connectors refacto ok
* [33m1e59168[m abstract connector :: wip
* [33mb22b2cc[m postgres ok
* [33m61b022e[m add postgres
* [33m4857bee[m make test
* [33m01bf715[m Tweak makefile
* [33meb5585b[m new fixtures
* [33m71aab3d[m wip
* [33m037e034[m Initial commit
