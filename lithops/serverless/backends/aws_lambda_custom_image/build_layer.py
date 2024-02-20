import json
import subprocess
import os
import zipfile
import boto3
import sys
import shutil
import glob
from botocore.errorfactory import ClientError
import zipfile
import platform

TEMP_PATH = '/tmp'
LAYER_DIR_PATH = os.path.join(TEMP_PATH, 'modules', 'python')
LAYER_ZIP_PATH = '/tmp/layer.zip'


def add_directory_to_zip(zip_file, full_dir_path, sub_dir=''):
    for file in os.listdir(full_dir_path):
        full_path = os.path.join(full_dir_path, file)
        if os.path.isfile(full_path):
            zip_file.write(full_path, os.path.join(sub_dir, file), zipfile.ZIP_DEFLATED)
        elif os.path.isdir(full_path) and '__pycache__' not in full_path:
            add_directory_to_zip(zip_file, full_path, os.path.join(sub_dir, file))

def install_dependencies(event):
    if os.path.exists(LAYER_DIR_PATH):
        if os.path.isdir(LAYER_DIR_PATH):
            shutil.rmtree(LAYER_DIR_PATH)
        elif os.path.isfile(LAYER_DIR_PATH):
            os.remove(LAYER_DIR_PATH)

    with open("/tmp/requirements.txt", "w") as file:
        for dep in event['dependencies']:
            file.write(f"{dep}\n")

    command = [sys.executable, '-m', 'pip', 'install', "-r", "/tmp/requirements.txt", "-t", LAYER_DIR_PATH]
    subprocess.check_call(command)


    # Remove 'tests' directories
    for root, dirs, files in os.walk(LAYER_DIR_PATH, topdown=False):
        for directory in dirs:
            if directory == 'tests':
                path = os.path.join(root, directory)
                shutil.rmtree(path)

    # Remove '__pycache__' directories
    for root, dirs, files in os.walk(LAYER_DIR_PATH, topdown=False):
        for directory in dirs:
            if directory == '__pycache__':
                path = os.path.join(root, directory)
                shutil.rmtree(path)

    # Remove '*.egg-info' and '*.dist-info' directories
    for root, dirs, files in os.walk(LAYER_DIR_PATH, topdown=False):
        for directory in dirs:
            if directory.endswith('.egg-info') or directory.endswith('.dist-info'):
                path = os.path.join(root, directory)
                shutil.rmtree(path)

    # Remove '*.pyc' files
    for root, dirs, files in os.walk(LAYER_DIR_PATH, topdown=False):
        for file in files:
            if file.endswith('.pyc'):
                path = os.path.join(root, file)
                os.remove(path)

    # Remove other specified packages
    other_packages_to_remove = [
        'caffe2', 'wheel', 'boto*', 'aws*', 'pip', 'pip-*', 'pipenv',
        'catalogue*', 'bs4*', 'srsly*', 'pydantic*', 'murmurhash*', 'click*', 'wasabi*', 'typer*',
        'smart*', 'preshed*', '[mM]arkup[sS]afe*', 'confection*', 'tzdata*', 'spacy*', 'soupsieve*',
        'setuptools*', 'python-dateutil*', 'pathy*', 'langcodes*', 'jinja2*', 'font[tT]ools*', 'contourpy*',
        'spacy*', 'nvidia*', 'numexpr*', '[bB]ottleneck*', 'beautifulsoup4*', 'torchaudio*', 'dateutil*',
        'jinja*', 'plac*', 'pydantic*',
        'lithops*', 'click*', 'cycler*', 'jmespath*', 'kiwisolver*', 'rsa*', 'seaborn*', 'PyJWT*',
        'requests_oauthlib*', 'oauthlib*', 'packaging*', 'bcrypt*', 'docker*', 'pyasn*', 'websocket_client*',
        'lxml*', 'PyNaCl*', 'paramiko*', 'cryptography*', 'setuptools*', 'jwt*', 'fonttools*',
        'pandas*', 'test*', '_cffi_backend.cpython-37m-x86_64-linux-gnu.so', 'mpl_toolkits', 'pkg_resources*',
        '*yaml*', '*Yaml*', 'nacl*', 'distutils-precedence.pth', 'matplotlib*', 'pylab*', 'share', 'pycparser*',
        'pyparsing*', 'cachetools*', 'cffi*', '_distutils_hack', 'pytz*', 'kubernetes*', 'ibm*', 'websocket*',
        '*dateutil*'
    ]
    for package in other_packages_to_remove:
        glob_path = os.path.join(LAYER_DIR_PATH, package)
        matching_packages = glob.glob(glob_path)
        for matching_package in matching_packages:
            if os.path.isdir(matching_package):
                shutil.rmtree(matching_package)
                print(f"Directory {matching_package} removed.")
            elif os.path.isfile(matching_package):
                os.remove(matching_package)
                print(f"File {matching_package} removed.")

    # Create a ZIP archive of the "torch" directory
    torch_path = os.path.join(LAYER_DIR_PATH, 'torch')




def lambda_handler(event, context):


    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=event['bucket'], Key=event['key'])
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The key does not exist.
            install_dependencies(event)

            torch_path = os.path.join(LAYER_DIR_PATH, 'torch')
            tmp_torch_path = '/tmp/torch'
            os.mkdir(tmp_torch_path)
            shutil.copytree(torch_path, tmp_torch_path+"/torch")
            with zipfile.ZipFile('/tmp/torch.zip', "w", compression=zipfile.ZIP_DEFLATED, compresslevel=0) as zipf:
                for root, _, files in os.walk(tmp_torch_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        zipf.write(file_path, arcname=os.path.relpath(file_path, tmp_torch_path))

            pyversion = platform.python_version_tuple()[0] + platform.python_version_tuple()[1]
            with open(f'/tmp/torch.zip', 'rb') as torch_zip:
                s3.put_object(Body=torch_zip, Bucket=event['bucket'], Key=f'torch{pyversion}.zip')

            if os.path.isdir(torch_path):
                # shutil.make_archive(torch_path, 'zip', LAYER_DIR_PATH, 'torch')
                shutil.rmtree(torch_path)

            os.remove('/tmp/torch.zip')

            s3.download_file(event['bucket'], event['model_file'], os.path.join(TEMP_PATH, 'modules') + '/'+event['model_file'])

            with zipfile.ZipFile(LAYER_ZIP_PATH, 'w') as layer_zip:
                add_directory_to_zip(layer_zip, os.path.join(TEMP_PATH, 'modules'))

            print("Everything added to Zip")
            with open(LAYER_ZIP_PATH, 'rb') as layer_zip:
                s3.put_object(Body=layer_zip, Bucket=event['bucket'], Key=event['key'])

            print("layer in S3")


        else:
            # Something else has gone wrong.
            return {
                'statusCode': e.response['Error']['Code'],
                'body': json.dumps(event)
            }
    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }