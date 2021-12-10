
You should create a new Connected Application with OAuth credentials in your Salesforce console.
[documentation](https://help.salesforce.com/articleView?id=connected_app_create.htm&type=5)

The scope must be set to api and refresh_token and the expiration time greater than 0 
to be able to have a refresh token in case of access token expiration.

For callback url, you should specify:

 **{{redirect_uri}}**
