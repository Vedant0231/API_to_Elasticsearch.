import requests
import json
from elasticsearch import Elasticsearch
import uuid
from mapping.mapping import mj_mappings

"""api call """
headers = {
    'authority': 'discord.com',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'authorization': 'MTA2ODkzMTMzMTg2NzgyNDE5OQ.GF0TrH.yYe8Hv_jGk-a2s7oQuJvQwqWVjbvFy5aDJKyjM',
    'x-super-properties': 'eyJvcyI6Ik1hYyBPUyBYIiwiYnJvd3NlciI6IkNocm9tZSIsImRldmljZSI6IiIsInN5c3RlbV9sb2NhbGUiOiJlbi1HQiIsImJyb3dzZXJfdXNlcl9hZ2VudCI6Ik1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwXzE1XzcpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8xMDkuMC4wLjAgU2FmYXJpLzUzNy4zNiIsImJyb3dzZXJfdmVyc2lvbiI6IjEwOS4wLjAuMCIsIm9zX3ZlcnNpb24iOiIxMC4xNS43IiwicmVmZXJyZXIiOiJodHRwczovL3d3dy5nb29nbGUuY29tLyIsInJlZmVycmluZ19kb21haW4iOiJ3d3cuZ29vZ2xlLmNvbSIsInNlYXJjaF9lbmdpbmUiOiJnb29nbGUiLCJyZWZlcnJlcl9jdXJyZW50IjoiaHR0cHM6Ly93d3cuZ29vZ2xlLmNvbS8iLCJyZWZlcnJpbmdfZG9tYWluX2N1cnJlbnQiOiJ3d3cuZ29vZ2xlLmNvbSIsInNlYXJjaF9lbmdpbmVfY3VycmVudCI6Imdvb2dsZSIsInJlbGVhc2VfY2hhbm5lbCI6InN0YWJsZSIsImNsaWVudF9idWlsZF9udW1iZXIiOjE3MDQ1NywiY2xpZW50X2V2ZW50X3NvdXJjZSI6bnVsbH0=',
}

response = requests.get(
    'https://discord.com/api/v9/guilds/662267976984297473/messages/search?content=Isometric',
    headers=headers,
)

json_data = response.json()

final_data = []

"""filtering data"""
messages =json_data.get("messages")

for message in messages:

    for ele in message:

        content = ele.get('content')
        
        attachments = ele.get("attachments",[])
  
        if content:
         
            if len(attachments) > 0:
         
                for item in attachments:
         
                    final_data.append({**item, "content" : content})

"""create elasticsearch connection"""
es = Elasticsearch("http://127.0.0.1:9200")

es.indices.put_mapping(index="mj_index", body=mj_mappings)  


for row in final_data:

    doc = {
        "id":row["id"],
        "filename":row["filename"],
        "size":row["size"],
        "url":row["url"],
        "proxy_url":row["proxy_url"],
        "width":row["width"],
        "height":row["height"],
        "content_type":row["content_type"],
        "content":row["content"],
    }

    es.index(index= "mj_index", body= doc)
    es.indices.refresh(index="mj_index")


"""storing json data into file"""
with open(f'json_response/{uuid.uuid4()}.json', 'w') as file:
    json.dump(final_data, file)

print("done")    
