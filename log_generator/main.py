import time
import datetime
import random
import gzip
from faker import Faker
from google.cloud import storage
import os
import base64
import json

faker = Faker()

def generate_logs(event, context):
    log_format = "ELF"  # or "CLF", can be passed as an event argument
    num_lines = 100  # number of log lines, adjustable
    bucket_name = "apache-logs"  # GCS bucket where logs will be uploaded
    file_prefix = "generated_logs"

    timestr = time.strftime("%Y%m%d-%H%M%S")
    otime = datetime.datetime.now()
    out_file_name = f'{file_prefix}_access_log_{timestr}.log'

    # GCS storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(out_file_name)

    response = ["200", "404", "500", "301"]
    verb = ["GET", "POST", "DELETE", "PUT"]
    resources = ["/list", "/wp-content", "/wp-admin", "/explore", "/search/tag/list", "/app/main/posts", "/posts/posts/explore", "/apps/cart.jsp?appID="]
    ualist = [faker.firefox, faker.chrome, faker.safari, faker.internet_explorer, faker.opera]

    # Writing the logs to a string
    log_data = ""
    for _ in range(num_lines):
        ip = faker.ipv4()
        dt = otime.strftime('%d/%b/%Y:%H:%M:%S')
        tz = datetime.datetime.now().strftime('%z')
        vrb = random.choice(verb)
        uri = random.choice(resources)
        if "apps" in uri:
            uri += str(random.randint(1000, 10000))
        resp = random.choice(response)
        byt = int(random.gauss(5000, 50))
        referer = faker.uri()
        useragent = random.choice(ualist)()

        if log_format == "CLF":
            log_data += f'{ip} - - [{dt} {tz}] "{vrb} {uri} HTTP/1.0" {resp} {byt}\n'
        else:
            log_data += f'{ip} - - [{dt} {tz}] "{vrb} {uri} HTTP/1.0" {resp} {byt} "{referer}" "{useragent}"\n'

        otime += datetime.timedelta(seconds=random.randint(30, 300))

    # Upload logs to GCS
    blob.upload_from_string(log_data)
    print(f'Uploaded log to {out_file_name}')
