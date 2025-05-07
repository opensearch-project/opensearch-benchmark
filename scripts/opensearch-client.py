import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import bulk
import json
import boto3
from requests_aws4auth import AWS4Auth
from opensearchpy.exceptions import NotFoundError


# Configuration for the client
host = 'opense-clust-0wEa2jJaumpT-72b822874267d2ec.elb.us-east-1.amazonaws.com'  # Replace with your OpenSearch host
port = 80         # Replace with your OpenSearch port
region = os.environ.get('AWS_REGION', 'us-east-1')
service = 'aoss'  # OpenSearch Serverless
# For AWS OpenSearch Service, you might need auth:
# auth = ("username", "password")  # For basic auth
# For AWS OpenSearch Service with IAM:
# from requests_aws4auth import AWS4Auth
# credentials = boto3.Session().get_credentials()
# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 'us-east-1', 'es')

# Create the client
#client = OpenSearch(
#    hosts=[{'host': host, 'port': port}],
    # If using SSL/TLS:
    # use_ssl=True,
    # verify_certs=True,
    # ca_certs='/path/to/ca_cert.pem',
    # For AWS OpenSearch Service with IAM:
    # http_auth=awsauth,
    # connection_class=RequestsHttpConnection,
    # If using self-signed certificates:
    # ssl_assert_hostname=False,
    # ssl_show_warn=False,
#)

# Check if the cluster is running
#print(f"Cluster info: {client.info()}")

# Get credentials automatically from the environment variables
session = boto3.Session()
credentials = session.get_credentials()
auth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=credentials.token
)

# Your OpenSearch Serverless collection endpoint
host = 'bfxcs01yjnxbnel2rqp5.us-east-1.aoss.amazonaws.com'  # Replace with your actual endpoint

# Create the client
client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    pool_maxsize=20,
)

try:
    # Check if the cluster is running
    info = client.info()
except NotFoundError as e:
    print(f"Connection error: {e.status_code}")

# Create an ind