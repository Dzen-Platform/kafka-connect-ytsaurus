import requests
import base64
import json
import random
import argparse
import time
import sys

def some_json():
    return {
        "age": "28 years old",
        "home": {
            "country": "Belize",
            "address": "314 example street"
        },
        "friends": [
            "Dannie, Pennie",
            "Amara, Aurlie",
            "Brit, Jillian",
            "Blinni, Yetty",
            "Deeanne, Florrie"
        ]
    }

def wait_for_kafka_rest(url, timeout=300):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)
    return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Send random JSON data to a Kafka topic.')
    parser.add_argument('--kafka-rest-url', default='http://localhost:8082', help='Kafka REST root URL (default: http://localhost:8082)')
    parser.add_argument('--topic', default='alala', help='Kafka topic to send data to (default: alala)')
    args = parser.parse_args()

    url = f"{args.kafka_rest_url}/topics/{args.topic}"
    headers = {
        "Content-Type" : "application/vnd.kafka.json.v2+json",
        "Accept": "application/vnd.kafka.v2+json",
    }


    if not wait_for_kafka_rest(args.kafka_rest_url):
        print("Kafka REST not available, exiting.")
        sys.exit(1)

    headers = {
        "Content-Type": "application/vnd.kafka.json.v2+json",
        "Accept": "application/vnd.kafka.v2+json",
    }

    num = 0
    while True:
        num += 1
        payload = {"records":
            [{
                "key": f"msg{i}",
                "value": { "i": i, "num": num, "data": some_json() },
            } for i in range(10)],
        }
        r = requests.post(url, data=json.dumps(payload), headers=headers)
        if r.status_code != 200:
            print("Status Code: " + str(r.status_code))
            print(r.text)
        else:
            print('done', num)
