import requests
from datetime import datetime, timedelta
import jwt
import base64

secret = ''
# The secret provided by ROK is b64 encoded, we need to decode it for jwt
notb64_secret = base64.b64decode(secret)

# Here we provide the registered claims and the claims we agreed on with ROK
data = {
    'aud': 'Rok-solution',
    'iss': '',
    'exp': str(int((datetime.now() + timedelta(minutes=10)).timestamp())),
    'email': '',
    'iat': str(int(datetime.now().timestamp())),
    'nbf': str(int(datetime.now().timestamp())),
}

token = jwt.encode(data, notb64_secret, algorithm='HS256')
response = requests.post('https://demo.rok-solution.com/graphql',
                         data='{\'query\':\'\'}',
                         headers={'DatabaseName': '', 'JwtString': token,
                                  'Accept': 'application/json', 'Content-Type': 'application/json'}).text

print(response)
