import requests
import json

# Define the endpoint and payload for the first POST request
url_1 = "http://localhost:9200/_plugins/_knn/models/3/_train"
payload_1 = {
    "training_index": "train-index",
    "training_field": "train_field_name",
    "dimension": 10,
    "description": "My model description",
    "method": {
        "name": "ivf",
        "engine": "faiss",
        "space_type": "l2",
        "parameters": {
            "nlist": 10,
            "nprobes": 5
        }
    }
}

headers_1 = {
    "Content-Type": "application/json"
}

# Send the first POST request
url_2 = "http://localhost:9200/_plugins/_knn/models/3"

response_1 = requests.post(url_1, headers=headers_1, data=json.dumps(payload_1))
# print("First response status:", response_1.status_code)
# print(response_1.text)

# # Define the endpoint for the second GET request

# # Send the second GET request
response_2 = requests.get(url_2)
print("Second response status:", response_2.status_code)

# Print the response from the second request
print("Second response content:")
print(response_2.text)
print(response_2)