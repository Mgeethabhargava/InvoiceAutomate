import json
import requests
from datetime import date, datetime
from azure.storage.blob import BlobServiceClient, BlobClient
import uuid
import os
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.core.pipeline.transport import RequestsTransport


# Function to convert date fields to strings
def convert_dates(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, (date, datetime)):
                obj[key] = value.isoformat()
            elif isinstance(value, dict):
                convert_dates(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        convert_dates(item)
    elif isinstance(obj, list):
        for i in range(len(obj)):
            if isinstance(obj[i], dict):
                convert_dates(obj[i])
# Configuration
sharepoint_raw_files_url = r"https://datastore15082024.blob.core.windows.net/rawfiles?sp=racwd&st=2024-08-16T17:01:43Z&se=2024-08-31T01:01:43Z&sv=2022-11-02&sr=c&sig=aNlYSTnMnXRdlrIOHsH9KklLKL%2BglZTMdAsTc%2Fv0bS8%3D"
sharepoint_good_files_url = r"https://datastore15082024.blob.core.windows.net/goodfiles?sp=racwdli&st=2024-08-16T14:46:53Z&se=2024-08-29T22:46:53Z&sv=2022-11-02&sr=c&sig=18O4ydYVcxxfPhZPFr1PYcPC3Zag4S35omCtPUWxAHI%3D"
sharepoint_bad_files_url = r"https://datastore15082024.blob.core.windows.net/badfiles?sp=racwdli&st=2024-08-16T14:47:42Z&se=2024-08-29T22:47:42Z&sv=2022-11-02&sr=c&sig=v8GoVfZYO4vlRtqEOAXHHSkBvp%2Ff16wFiwaNvyfgFQ8%3D"
azure_storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=datastore15082024;AccountKey=/NK134K92JbOq/NXisjVTq+w8RcgaG+cbOXhIakeJO/2nZhZrnh+JdEQlNowtUpu0buj4zSO47nX+AStxEWbmg==;EndpointSuffix=core.windows.net"
cosmos_endpoint = "https://db15082024.documents.azure.com:443/"
cosmos_key = "2ipSGPEFg31du8r4tHyJ8KksLCG2RIsijx3ZbDws67IF0vLRCBcoAxTsWO1XFQkCJpbNQ2onRGB6ACDbLbtpfA=="
cosmos_database_name = "Invoice"
cosmos_container_name = "Extractedata"
document_intelligence_endpoint = "https://invoiceparser15082024.cognitiveservices.azure.com/"
document_intelligence_key = "e908e847d16d4abea689bd97d6a6a59d"

# Initialize clients
blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
cosmos_client = CosmosClient(cosmos_endpoint, cosmos_key)
database = cosmos_client.get_database_client(cosmos_database_name)
container = database.get_container_client(cosmos_container_name)
document_analysis_client = DocumentAnalysisClient(
    endpoint=document_intelligence_endpoint,
    credential=AzureKeyCredential(document_intelligence_key)
)

def process_file():
    # Upload to Azure Storage
    container_client = blob_service_client.get_container_client("rawfiles")
    blobs_list = container_client.list_blobs()
    # Print out the blob names
    for blob in blobs_list:
        print(blob.name)
        # Get the BlobClient
        blob_client = blob_service_client.get_blob_client(container="rawfiles", blob=blob.name)
        # Download the blob's content
        download_stream = blob_client.download_blob()
        print("File Downloaded")
        # Read the content
        blob_data = download_stream.readall()
        # Extract data using Azure Document Intelligence
        print("parsing the data from attachment")
        try:
            poller = document_analysis_client.begin_analyze_document(
                model_id="prebuilt-invoice",
                document=blob_data
            )
            result = poller.result()
            result_dict = result.to_dict().get("documents",None)
            if result_dict:
                print("no of dicts:", len(result_dict))
                # print("response 1:",result_dict)
                result_dict = result_dict[0].get("fields",None)
                # print("response 2:",result_dict)
                convert_dates(result_dict)
                # print(result_dict)
                # Save to Cosmos DB      
                # Move file based on success or failure
                if result_dict:
                    # Move file to Good Files
                    try:
                        tracking_info = {
                            'id': str(uuid.uuid1()),  # Unique ID for the item  # Part
                            "file_name": blob.name,
                            "parsing_status":"Success",
                            "error_details" : ""
                            }
                        address = result_dict.get("BillingAddress",{}).get("value", {})
                        BilligAddressRecipient = result_dict.get("BilligAddressRecipient",{}).get("value", {})
                        Customername = result_dict.get("CustomerName",{}).get("value", "")
                        InvoiceDate = result_dict.get("InvoiceDate",{}).get("content", "")
                        InvoiceId = result_dict.get("InvoiceId",{}).get("value", "")
                        InvoiceTotal = result_dict.get("InvoiceTotal",{}).get("value", {})
                        Items = result_dict.get("Items",{}).get("value", {})
                        VendorAddress = result_dict.get("VendorAddress",{}).get("value", {})
                        VendorName = result_dict.get("VendorName",{}).get("value", "")
                        extracted_data = {"BillingAddress":address, "BilligAddressRecipient":BilligAddressRecipient,"CustomerName":Customername,"InvoiceDate":InvoiceDate,"InvoiceId":InvoiceId,
                                                           "InvoiceTotal":InvoiceTotal,"Items":Items,"VendorAddress":VendorAddress, "VendorName":VendorName}
                        # print(extracted_data)
                        tracking_info["extracted_data"] = json.dumps(extracted_data, indent=4)
                        container.create_item(body=tracking_info)
                        print("Successful Item inserted successfully!")
                    except exceptions.CosmosHttpResponseError as e:
                        print(f"An error occurred: {e.message}")
    
                    move_file(blob.name, blob_data,"goodfiles")
                else:
                    print(result_dict)
                    try:
                        tracking_info = {
                            'id': str(uuid.uuid1()),  # Unique ID for the item  # Part
                            "file_name": blob.name,
                            "extracted_data": "{}",
                            "parsing_status":"Failed",
                            "error_details" : "Parsing issue"
                            }  
                        container.create_item(body=tracking_info)
                        print("Failure Item inserted successfully!")
                    except exceptions.CosmosHttpResponseError as e:
                        print(f"An error occurred: {e.message}")
                    # Move file to Bad Files
                    move_file(blob.name, blob_data,"badfiles")
        except Exception as e:
                try:
                    tracking_info = {
                        'id': str(uuid.uuid1()),  # Unique ID for the item  # Part
                        "file_name": blob.name,
                        "extracted_data": "{}",
                        "parsing_status":"Failed",
                        "Error_details" : str(e)
                        }  
                    container.create_item(body=tracking_info)
                    print("Failure Item inserted successfully!", e)
                except exceptions.CosmosHttpResponseError as e:
                    print(f"An error occurred: {e.message}")
                # Move file to Bad Files
                move_file(blob.name, blob_data,"badfiles")

def move_file(filename, blobdata, container_name):
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(filename)
        blob_client.upload_blob(blobdata)
        container_client = blob_service_client.get_container_client("rawfiles")
        blob_client = container_client.get_blob_client(filename)
        # blob_client.delete_blob()
         # Implement the code to move file within SharePoint
    except Exception as e:
        print("Exception occured:", e)

# Example usage
process_file()
