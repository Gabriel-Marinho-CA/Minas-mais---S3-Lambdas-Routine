import http.client
import json
import boto3

s3 = boto3.client('s3')


def handler(event, context):
    try:
        data = upload_fn()
        response = {
            "statusCode": 200,
            "body": json.dumps(data)
        }
        return response
    except Exception as e:
        print(f"Error in handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


def getAuth():
    try:
        conn = http.client.HTTPSConnection("####.epharma.com.br")

        headers = {
            "username": "###",
            "password": "###",
            "client_id": "###",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        conn.request("GET", "/authentication/api/v1/OAuth/Authenticate", headers=headers)

        res = conn.getresponse()
        data = res.read().decode("utf-8")

        conn.close()

        if res.status != 200:
            raise Exception(f"Auth failed: {res.status} - {data}")

        return json.loads(data)
    except Exception as e:
        print(f"Error fetching auth from ePharma: {e}")
        return None


def get_data_from_epharma():
    auth = getAuth()
    if not auth:
        return None

    token = auth["data"]["token"]["accessToken"]

    try:
        conn = http.client.HTTPSConnection("####.epharma.com.br")

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        conn.request("GET", "/Client/api/v1/Client/Industry/Associated/04803404000174", headers=headers)

        res = conn.getresponse()
        data = res.read().decode("utf-8")

        conn.close()

        if res.status != 200:
            raise Exception(f"ePharma request failed: {res.status} - {data}")

        return json.loads(data)
    except Exception as e:
        print(f"Error fetching data from ePharma: {e}")
        return None


def upload_fn():
    try:
        data = get_data_from_epharma()
        if not data:
            return {"error": "Failed to fetch data from ePharma"}

        bucket_name = "teste-minas-mais-rotina"
        key_name = "data.json"

        s3.put_object(
            Bucket=bucket_name,
            Key=key_name,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        print("Upload Completed")
        return {"message": "Upload Completed", "bucket": bucket_name, "key": key_name}

    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return {"error": str(e)}
