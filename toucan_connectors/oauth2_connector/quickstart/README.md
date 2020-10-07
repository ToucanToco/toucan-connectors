## How to use Aircall Quickstart 

The Redirect URI is manually set by Aircall, for testing purpose edit your /etc/hosts file with: 
```
127.0.0.1 api-xxxxx.xxxxx.xxx 
```
Credentials (secret_id and secret_key) need to be added in the quickstart script as well as the redirect_uri.
During the dance, once redirected, visit the given redirect URI by rewriting it to http://xxxxxxxxxxx:35000/..... instead of https://xxxxxxxxx/...... 