import json
import boto3
import base64
import subprocess
import tempfile
import os
import urllib3

ssm_client = boto3.client('ssm')
http = urllib3.PoolManager()

def send_response(event, context, status, response_data, physical_resource_id):
    """Send response to CloudFormation"""
    response_body = json.dumps({
        'Status': status,
        'Reason': f'See CloudWatch Log Stream: {context.log_stream_name}',
        'PhysicalResourceId': physical_resource_id,
        'StackId': event['StackId'],
        'RequestId': event['RequestId'],
        'LogicalResourceId': event['LogicalResourceId'],
        'Data': response_data
    })
    
    headers = {'content-type': '', 'content-length': str(len(response_body))}
    
    try:
        response = http.request('PUT', event['ResponseURL'], headers=headers, body=response_body)
        print(f"CloudFormation response status: {response.status}")
    except Exception as e:
        print(f"send_response failed: {e}")

def handler(event, context):
    """Generate RSA key pair for CloudFront using openssl"""
    print(f"Event: {json.dumps(event)}")
    
    request_type = event['RequestType']
    environment = event['ResourceProperties']['Environment']
    service = event['ResourceProperties']['Service']
    
    # SSM parameter paths
    private_key_path = f"/{environment}/{service}/cloudfront/private-key"
    public_key_path = f"/{environment}/{service}/cloudfront/public-key"
    
    try:
        if request_type == 'Create':
            # Create temp directory for keys
            with tempfile.TemporaryDirectory() as tmpdir:
                private_key_file = os.path.join(tmpdir, 'private.pem')
                public_key_file = os.path.join(tmpdir, 'public.pem')
                
                # Generate RSA private key using openssl
                print("Generating RSA 2048-bit key pair...")
                subprocess.run([
                    'openssl', 'genrsa', '-out', private_key_file, '2048'
                ], check=True, capture_output=True, text=True)
                
                # Extract public key
                subprocess.run([
                    'openssl', 'rsa', '-in', private_key_file, '-pubout', '-out', public_key_file
                ], check=True, capture_output=True, text=True)
                
                # Read keys
                with open(private_key_file, 'r') as f:
                    private_key_pem = f.read()
                with open(public_key_file, 'r') as f:
                    public_key_pem = f.read()
            
            # Store private key in SSM (base64 encoded)
            private_key_b64 = base64.b64encode(private_key_pem.encode()).decode('utf-8')
            ssm_client.put_parameter(
                Name=private_key_path,
                Value=private_key_b64,
                Type='SecureString',
                Description='CloudFront private key for signing URLs (base64 encoded)',
                Overwrite=True
            )
            print(f"Stored private key in SSM: {private_key_path}")
            
            # Store public key in SSM (plain text)
            ssm_client.put_parameter(
                Name=public_key_path,
                Value=public_key_pem,
                Type='String',
                Description='CloudFront public key for signing URLs',
                Overwrite=True
            )
            print(f"Stored public key in SSM: {public_key_path}")
            
            response_data = {
                'PublicKey': public_key_pem,
                'PublicKeySSMPath': public_key_path,
                'PrivateKeySSMPath': private_key_path
            }
            
            physical_resource_id = f"cf-keys-{environment}-{service}"
            send_response(event, context, 'SUCCESS', response_data, physical_resource_id)
            
        elif request_type == 'Update':
            # Return existing public key without regenerating
            try:
                response = ssm_client.get_parameter(Name=public_key_path)
                public_key_pem = response['Parameter']['Value']
                
                response_data = {
                    'PublicKey': public_key_pem,
                    'PublicKeySSMPath': public_key_path,
                    'PrivateKeySSMPath': private_key_path
                }
                
                send_response(event, context, 'SUCCESS', response_data, event['PhysicalResourceId'])
            except ssm_client.exceptions.ParameterNotFound:
                # Keys don't exist, treat as Create
                print("Keys not found, creating new keys...")
                event['RequestType'] = 'Create'
                return handler(event, context)
                
        elif request_type == 'Delete':
            # Keep keys in SSM for safety
            print(f"Delete requested. Keys retained in SSM: {private_key_path}, {public_key_path}")
            
            response_data = {'Message': 'Keys retained in SSM'}
            physical_resource_id = event.get('PhysicalResourceId', f"cf-keys-{environment}-{service}")
            send_response(event, context, 'SUCCESS', response_data, physical_resource_id)
            
    except Exception as e:
        print(f"Error: {str(e)}")
        send_response(event, context, 'FAILED', {'Error': str(e)}, 
                     event.get('PhysicalResourceId', 'failed-resource'))