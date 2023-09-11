import boto3
import csv
from datetime import datetime

def lambda_handler(event, context):
#initialize the s3 resource using boto3.
    s3 = boto3.client('s3')
#extract the bucket name and csv file name from 'event' input
    billing_bucket = event['Records'][0]['s3']['bucket']['name']
    csv_file = event['Records'][0]['s3']['object']['key']
#Defining the name of error bucket where you want to copy the error csv files
    error_bucket = 'malini-billing-error-x'
    
#download the csv file from s3, read the contend, decode from bytes to sring, and split the content by lines.
    obj = s3.get_object(billing_bucket, csv_file)
    data = obj.get()['Body'].read().decode('utf-8').splitlines()
#Initialize a flag (error_found) to false. We'll set this flag to true when we find an error.
    error_found = False
# Define Valid product lines and valid currencies
    valid_product_lines = ['Bakery', 'Meat', 'Dairy']
    valid_currencies = ['USD', 'MXI', 'CAD']
#Read the CSV content line by line using python's csv reader. Ignore the header line(data[1:])
    for row in csv.reader(data[1:], delimiter= ','):
        #print(f"{row}")
        date = row[6]
        product_line = row[4]
        currency = row[7]
        bill_amount = float(row[8])
#check if the product line is valid. If not, set error flag to true and print an error message
        if product_line not in valid_product_lines:
            error_found = True
            print(f"Error in record {row[0]}: Unrecognized product line: {product_line}.")
            break
# check if the currenty is valid. If not, set error flag to true and print an error message
        if currency not in valid_currencies:
            error_found = True
            print(f"Error in record {row[0]}: Unrecognized currency: {currency}.")
            break
#Check if the date is in the correct format* %y-%m-%d) if not, set error flag to tru and print an error message
        try:
            datetime.strptime(date, '%y-%m-%d')
        except ValueError:
            error_found = True
            print(f"Error in record {row[0]}: incorrect date format: {date}.")
        break
       

#After checking all rows, if an error is found, copy the csv file to the error bucket and delete it from the original bucket
    if error_found:
        copy_source = {
            'Bucket': billing_bucket,
            'Key': csv_file
        }
        try:
            s3.meta.client.copy(copy_source, error_bucket, csv_file)
            print(f"Moved error file to: {error_bucket}.")
            s3.Object(billing_bucket, csv_file).delete()
            print("deleted original file from bucket.")

#Handle any exception that may occur while moving the file , and print the error message
        except Exception as e:
            print(f"Error while moving file: {str(e)}.")
#if not error were fond, return a success message wit hstatus code 200 and body message indicating that no errors were found
    else:
      return {
            'statusCode': 200,
            'body': ('Hello from Lambda!')
        }

# Check if the bill amount is negative. If so, set eorror flag to tru and print an error message.

    
       
