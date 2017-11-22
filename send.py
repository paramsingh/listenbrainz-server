import json
import requests
import time

url = 'http://0.0.0.0/1/submit-listens'

auth = '82e0b37d-8c99-4fe5-9c3b-31f95c58856f'
doc = {
    "listen_type": "import",
    "payload": [
        {
            "listened_at": int(time.time()),
            "track_metadata": {
                "artist_name": "Kanye West",
                "release_name": "The Life of Pablo",
                "track_name": "Fade"
            }
        }
    ]
}


if __name__ == '__main__':

    r = requests.post(
        url,
        data=json.dumps(doc),
        headers={'Authorization': 'Token {}'.format(auth)},
    )

    print(r.status_code)
