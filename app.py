from flask import Flask, request, redirect, url_for, render_template, flash, get_flashed_messages
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.cosmos import exceptions, CosmosClient, PartitionKey
import os
import json
import re
import traceback

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Change this to a secret key for session management

# Azure Blob Storage configuration
AZURE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=datastore15082024;AccountKey=/NK134K92JbOq/NXisjVTq+w8RcgaG+cbOXhIakeJO/2nZhZrnh+JdEQlNowtUpu0buj4zSO47nX+AStxEWbmg==;EndpointSuffix=core.windows.net'
CONTAINER_NAME = 'rawfiles'

# Azure Cosmos DB configuration
ENDPOINT = 'https://db15082024.documents.azure.com:443/'
PRIMARY_KEY = '2ipSGPEFg31du8r4tHyJ8KksLCG2RIsijx3ZbDws67IF0vLRCBcoAxTsWO1XFQkCJpbNQ2onRGB6ACDbLbtpfA=='
DATABASE_NAME = 'Invoice'
CONTAINER_NAME = 'extractedata'


# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

client = CosmosClient(ENDPOINT, PRIMARY_KEY)
database = client.get_database_client(DATABASE_NAME)
container = database.get_container_client(CONTAINER_NAME)


# Home Route
@app.route('/', methods=['GET'])
def home():
    items = list(container.read_all_items())
    return render_template('index.html', items=items)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part')
        print("no file part")
        return redirect(request.url)
    
    file = request.files['file']
    
    if file.filename == '':
        flash('No selected file')
        print("No selected file")
        return redirect(request.url)
    
    if file:
        blob_client = container_client.get_blob_client(file.filename)
        blob_client.upload_blob(file, overwrite=True)
        flash('File successfully uploaded to Azure Blob Storage!')
        print("File successfully uploaded to Azure Blob Storage!")
        return redirect(url_for('index'))

# Read Route
@app.route('/item/<item_id>')
def read_item(item_id):
    try:
        item = container.read_item(str(item_id), partition_key=str(item_id))
        if item.get("parsing_status", "")=="Success":
            name = item.get("file_name", "")
            item["url"] = f"https://datastore15082024.blob.core.windows.net/goodfiles/{name}"            
            return render_template('view_item.html', item=item)
        else:
            name = item.get("file_name", "")
            item["url"] = f"https://datastore15082024.blob.core.windows.net/badfiles/{name}"            
            return render_template('view_item.html', item=item)
    except exceptions.CosmosResourceNotFoundError as e:
        print(e)
        return f'Item with id {item_id} not found', 404

# move_to_badfile Route
@app.route('/badfile/<item_id>', methods=['POST'])
def move_to_badfile(item_id):
    updated_data = request.form['updated_value']
    # print(updated_data)
    updated_data = re.sub(r"(\")", r"**", updated_data)
    # print(updated_data)
    updated_data = str(updated_data).replace("'", '"')
    updated_data = str(updated_data).replace("**", "'")
    # print(updated_data)
    print(type(updated_data))
    updated_data = re.sub(r'\n', ' ', updated_data)
    # updated_data = '"""'+updated_data+'"""'
    updated_data = json.loads(updated_data)
    print(type(updated_data))
    try:
        try: 
            print(type(updated_data))
            blob_name = updated_data.get("url").split("/")[-1]
            parsing_status = updated_data.get("parsing_status")
            if parsing_status.lower()=="failed":
                folder = "badfiles"
            elif parsing_status.lower()=="success":
                folder = "goodfiles"
            print(folder, blob_name)
            # Initialize source and destination container clients
            source_container_client = blob_service_client.get_container_client(folder)
            destination_container_client = blob_service_client.get_container_client("badfiles")
            # Initialize BlobClient for the source blob
            source_blob_client = source_container_client.get_blob_client(blob_name)
            # Read the blob content from the source container
            blob_data = source_blob_client.download_blob().readall()
            # Initialize BlobClient for the destination blob
            destination_blob_client = destination_container_client.get_blob_client(blob_name)
            # Upload the blob content to the destination container
            destination_blob_client.upload_blob(blob_data, overwrite=True)
        except Exception as e:
           print(traceback.format_exc())
           print("Error occured at file moving:",e)
        updated_data["parsing_status"] = "Failed"
        print(updated_data)
        # Delete the blob from the source container
        source_blob_client.delete_blob()
        item = container.read_item(item_id, partition_key=item_id)
        item.update(updated_data)
        container.upsert_item(item)
        print("Success work at good files:", item_id)
        print("DB Updated Successfully")
    except Exception as e:
        print(e)
        print("Error occured at DB Updating")
    return redirect(url_for('home'))

# Update Route
@app.route('/correctfile/<item_id>', methods=['POST'])
def move_to_correctfile(item_id):
    updated_data = request.form['updated_value']
    print(updated_data)
    updated_data = re.sub(r"(\")", r"**", updated_data)
    print(updated_data)
    updated_data = str(updated_data).replace("'", '"')
    updated_data = str(updated_data).replace("**", "'")
    print(updated_data)
    print(type(updated_data))
    updated_data = re.sub(r'\n', ' ', updated_data)
    # updated_data = '"""'+updated_data+'"""'
    updated_data = json.loads(updated_data)
    print(type(updated_data))
    try:
        try:
            blob_name = updated_data.get("url").split("/")[-1]
            parsing_status = updated_data.get("parsing_status")
            if parsing_status.lower()=="failed":
                folder = "badfiles"
            elif parsing_status.lower()=="success":
                folder = "goodfiles"
            print(folder, blob_name)            
            # Initialize source and destination container clients
            source_container_client = blob_service_client.get_container_client(folder)
            destination_container_client = blob_service_client.get_container_client("goodfiles")
            # Initialize BlobClient for the source blob
            source_blob_client = source_container_client.get_blob_client(blob_name)
            # Read the blob content from the source container
            blob_data = source_blob_client.download_blob().readall()
            # Initialize BlobClient for the destination blob
            destination_blob_client = destination_container_client.get_blob_client(blob_name)
            # Upload the blob content to the destination container
            destination_blob_client.upload_blob(blob_data, overwrite=True)
        except Exception as e:
           print("Error occured at file moving:",e)
        updated_data["parsing_status"] = "Success"
        print(updated_data)
        # Delete the blob from the source container
        source_blob_client.delete_blob()
        item = container.read_item(item_id, partition_key=item_id)
        item.update(updated_data)
        container.upsert_item(item)
        print("Success work at good files:", item_id)
        print("DB Updated Successfully")
    except Exception as e:
        print(e)
        print("Error occured at DB Updating")
    return redirect(url_for('home'))

# Delete Route
@app.route('/delete/<item_id>')
def delete_item(item_id):
    try:
        container.delete_item(item_id, partition_key=item_id)
        return redirect(url_for('home'))
    except exceptions.CosmosResourceNotFoundError:
        return f'Item with id {item_id} not found', 404

if __name__ == '__main__':
    app.run(debug=True)
