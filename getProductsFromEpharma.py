import http.client
import json
import boto3

# ===============================
# CONFIGURAÇÕES GLOBAIS
# ===============================

# Domínio ePharma
EPHARMA_DOMAIN = "####.epharma.com.br"

# Credenciais fixas de autenticação
EPHARMA_AUTH_HEADERS = {
    "username": "###",
    "password": "###",
    "client_id": "###",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# Endpoint fixo da consulta de dados
EPHARMA_CLIENT_ENDPOINT = "/Client/api/v1/Client/Industry/Associated/04803404000174"

# Bucket de destino S3
S3_BUCKET = "teste-minas-mais-rotina"
S3_KEY = "data.json"

# Cliente S3 global
s3 = boto3.client('s3')


# ===============================
# HANDLER PRINCIPAL (AWS Lambda)
# ===============================

def handler(event, context):
    try:
        data = upload_fn()
        return {
            "statusCode": 200,
            "body": json.dumps(data)
        }
    except Exception as e:
        print(f"Error in handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


# ===============================
# FUNÇÕES AUXILIARES
# ===============================

def getAuth():
    """Obtém token de autenticação da ePharma."""
    try:
        conn = http.client.HTTPSConnection(EPHARMA_DOMAIN)
        conn.request("GET", "/authentication/api/v1/OAuth/Authenticate", headers=EPHARMA_AUTH_HEADERS)

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
    """Busca dados da ePharma usando o token obtido."""
    auth = getAuth()
    if not auth:
        return None

    token = auth["data"]["token"]["accessToken"]

    try:
        conn = http.client.HTTPSConnection(EPHARMA_DOMAIN)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        conn.request("GET", EPHARMA_CLIENT_ENDPOINT, headers=headers)

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
    """Busca dados da ePharma e salva no bucket S3."""
    try:
        data = get_data_from_epharma()
        if not data:
            return {"error": "Failed to fetch data from ePharma"}

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        print("Upload Completed")
        return {
            "message": "Upload Completed",
            "bucket": S3_BUCKET,
            "key": S3_KEY
        }

    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return {"error": str(e)}
