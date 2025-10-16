import json
import boto3
import http.client

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        data = read_json_file_from_bucket("minas-mais-teste-2","data.json")
        products_by_ean = get_products_by_eans(data)
        update_products(products_by_ean)
        
        response = {
            "statusCode": 200,
            "body": "Alterado com sucesso"
        }
        return response
    except Exception as e:
        print(f"Error in handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


def read_json_file_from_bucket(bucket_name, file_name):
    s3_client = boto3.client('s3')

    # Lê o arquivo JSON do bucket
    s3_response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    file_data = s3_response["Body"].read().decode('utf-8')

    # Converte o conteúdo em dicionário Python
    data = json.loads(file_data)

    # Extrai EANs e seus respectivos benefitIds
    eans = []
    for item in data.get("data", []):
        benefit = item.get("benefit", {})
        benefit_id = benefit.get("id")
        products = benefit.get("products", [])

        for product in products:
            ean = product.get("ean")
            if ean:
                eans.append({
                    "benefitId": benefit_id,
                    "ean": ean
                })

    return eans
    


def get_products_by_eans(eans):
    conn = http.client.HTTPSConnection("###.myvtex.com")

    headers = {    
        'X-VTEX-API-AppKey': "###",
        'Content-Type': "application/json",
        'X-VTEX-API-AppToken': "###"
    }

    results = []

    for item in eans:
        ean = item["ean"]
        benefit_id = item["benefitId"]

        try:
            endpoint = f"/api/catalog_system/pub/products/search?fq=alternateIds_Ean:{ean}"
            conn.request("GET", endpoint, headers=headers)
            res = conn.getresponse()
            data = res.read().decode("utf-8")
            product_data = json.loads(data)

            if not product_data:
                results.append({
                    "ean": ean,
                    "benefitId": benefit_id,
                    "error": "Produto não encontrado"
                })
                continue

            product_info = product_data[0]

            results.append({
                "ean": ean,
                "benefitId": benefit_id,
                "productId": product_info.get("productId"),
                "productName": product_info.get("productName"),
                "categoryId": product_info.get("CategoryId") or product_info.get("categoryId"),
                "brandId": product_info.get("brandId")
            })

        except Exception as e:
            print(f"Erro ao buscar EAN {ean}: {e}")
            results.append({
                "ean": ean,
                "benefitId": benefit_id,
                "error": str(e)
            })

    conn.close()
    return results

def update_products(products):
    conn = http.client.HTTPSConnection("####.vtexcommercestable.com")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        'X-VTEX-API-AppKey': "###",
        'X-VTEX-API-AppToken': "###"
    }

    results = []

    for product in products:
        try:
            product_id = product.get("productId")
            benefit_id = product.get("benefitId")
            ean = product.get("ean")

            if not product_id or not benefit_id:
                results.append({
                    "ean": ean,
                    "status": "missing_fields"
                })
                continue

            payload = [
                {
                    "Value": [str(benefit_id)],
                    "Name": "beneficiaryId"
                }
            ]

            conn.request(
                "POST",
                f"/api/catalog/pvt/product/{product_id}/benefit",
                body=json.dumps(payload),
                headers=headers
            )

            res = conn.getresponse()
            data = res.read().decode("utf-8")

            results.append({
                "ean": ean,
                "productId": product_id,
                "status": res.status,
                "response": data
            })

        except Exception as e:
            print(f"Erro ao atualizar produto {ean}: {e}")
            results.append({
                "ean": ean,
                "error": str(e)
            })

    conn.close()
    return results
