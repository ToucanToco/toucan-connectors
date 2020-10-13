## How to use Aircall Quickstart 

This quickstart script is located in doc/connectors
The Redirect URI is manually set by Aircall, for testing purpose edit your /etc/hosts file with: 
```
127.0.0.1 your-redirect-uri-domain
```
Credentials (secret_id and secret_key) need to be added in the quickstart script as well as the redirect_uri.
During the authorization process, once redirected, visit the given redirect URI by rewriting it to http://xxxxxxxxxxx:35000/..... instead of https://xxxxxxxxx/......