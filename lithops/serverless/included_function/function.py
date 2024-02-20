def lambda_function(payload):
    print(payload)
    # time.sleep(5)
    return {
        'statusCode': 200,
        'body': "Function just returns after loading dependencies"
    }