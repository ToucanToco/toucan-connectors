* A quick readme for what I implemented * 

I tried to understand as much as I could the logic behind the ToucanConnector and ToucanDataSource class to implement them with my version of the GithubConnector/GithubDataSource as indicated in the Toucan's repo documentation

I focused on building tests before actually implementing the classes in toucan-connectors/tests/github/test_github.py, I found cases which seemed relevant to me as, no token defined or invalid token, empty query or invalid query and finally test of query to get last 20 open PR

Tests can be ran as below:
`
pytest tests/github
`

I would glad to have your feedback on my proposal !

Thanks & Regards,

Raphaël