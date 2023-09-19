# GoogleBigQuery connector

## Data provider configuration

* `type`: `"GoogleBigQuery"`
* `name`: str, required
* `credentials`: GoogleCredentials, required
* `dialect`: Dialect, default to legacy
* `scopes`: list(str), default to ["https://www.googleapis.com/auth/bigquery"]

### GoogleCredentials

For authentication, download an authentication file from console.developper.com
and use the values here. This is an oauth2 credential file. For more information
see this: http://gspread.readthedocs.io/en/latest/oauth2.html

* `type`: str
* `project_id`: str
* `private_key_id`: str
* `private_key`: str
* `client_email`: str
* `client_id`: str
* `auth_uri`: str
* `token_uri`: str
* `auth_provider_x509_cert_url`: str
* `client_x509_cert_url`: str
* `jwt_token`: str

You can also authenticate with a `signed jwt_token` you've created yourself,
in that case, you will only need here two fields:

- `project_id`: str
- `jwt_token`: str

*Note*: you will be responsible on updating the `jwt_token` when it expired or
set a hight value for the expiration.

### HOW TO CREATE A GOOGLE_AUTH JWT

Using python, you can generate a JWT with a `service_account_secret_file.json`,
like this:
```python

import time
from google.auth  import crypt, jwt

def generate_jwt(
    sa_keyfile,
    sa_email="account@project-id.iam.gserviceaccount.com",
    audience="your-service-name",
    expiry_length=3600,
):
    """Generates a signed JSON Web Token using a Google API Service Account."""

    now = int(time.time())

    # build payload
    payload = {
        "iat": now,
        "exp": now + expiry_length,
        "iss": sa_email,
        "aud": audience,
        "sub": sa_email,
        "email": sa_email,
    }

    # sign with keyfile
    signer = crypt.RSASigner.from_service_account_file(sa_keyfile)
    jwt_token = jwt.encode(signer, payload)

    return jwt_token

print(generate_jwt(
    './service_account_secret.json',
    sa_email="your-email@biquery-integration-tests.iam.gserviceaccount.com",
    audience="https://bigquery.googleapis.com/",
    expiry_length=3600 # feel free to set the value you want here
).decode('utf-8'))
```

With the `service_account_secret.json` structured as follow :
```json
{
  "type": "service_account",
  "project_id": "---",
  "private_key_id": "---",
  "private_key": "---",
  "client_email": "your-email@biquery-integration-tests.iam.gserviceaccount.com",
  "client_id": "---",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bigquery-xxxx"
}
```
and then use it in the GoogleCredentials form.


```coffee
DATA_PROVIDERS: [
  type:    'GoogleBigQuery'
  name:    '<name>'
  credentials:    '<credentials>'
  dialect:    '<dialect>'
  scopes:    '<scopes>'
,
  ...
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required
* `query`: str, required

```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  query:    '<query>'
,
  ...
]
```
