import os
import subprocess
import urllib.parse

import boto3

ssh_cert = os.environ["ssh_key_file"]
ssh_username = os.environ["ssh_username"]

if not os.path.isfile("bi-emr2.pem"):
    with open(ssh_cert, "r") as myfile:
        rsa_contents = myfile.read()

    with open("bi-emr2.pem", "w") as myfile:
        myfile.write(rsa_contents)

ec2 = boto3.client("ec2")
response = ec2.describe_instances(
    Filters=[{"Name": "tag:Hostname", "Values": ["ue00mdaapp01"]}]
)

for rsvn in response.get("Reservations"):
    if "PrivateIpAddress" in rsvn["Instances"][0]:
        host_ip = rsvn["Instances"][0]["PrivateIpAddress"]
        break

print("Using EC2 IpAddress of " + host_ip)

subprocess.check_call(["chmod", "400", "bi-emr2.pem"])

pass_url = urllib.parse.quote(os.environ["bitbucket_pass"])

command_list = [
    "sudo yum install git -y",
    "sudo yum install python3-pip -y",
    "sudo python3 -m pip install pyarrow==8.0.0",
    "sudo python3 -m pip install joblib==1.1.0",
    "sudo python3 -m pip install boto3==1.26.5",
    "sudo python3 -m pip install numpy==1.21.6",
    "sudo python3 -m pip install s3io==0.1.1",
    "sudo python3 -m pip install pyaml==21.10.1",
    "sudo python3 -m pip install pandas==1.3.5",
    "sudo python3 -m pip install scikit-learn==0.24.2",
]

for comm in command_list:
    cmd = [
        "ssh",
        "-o StrictHostKeyChecking=no",
        "-i",
        "bi-emr2.pem",
        "ec2-user" + "@" + host_ip,
        comm,
    ]

    try:
        result = subprocess.check_output(cmd)
        print(result)
    except subprocess.CalledProcessError as e:
        print(e.output)
        raise Exception("error when running " + str(cmd))


cmd = [
    "scp",
    "-i",
    "bi-emr2.pem",
    "-r",
    "memberdna/",
    "ec2-user" + "@" + host_ip + ":memberdna/",
]
result = subprocess.check_output(cmd)
print(result)

cmd = [
    "ssh",
    "-o StrictHostKeyChecking=no",
    "-i",
    "bi-emr2.pem",
    "ec2-user" + "@" + host_ip,
    "cd memberdna/pipelines/trip_spend_model ; export PYTHONPATH=/home/ec2-user ; make models_predict > models_predict.txt ",
]

try:
    result = subprocess.check_output(cmd)
    print(result)
except subprocess.CalledProcessError as e:
    print(e.output)
    raise Exception("error when running " + str(cmd))

cmd = [
    "ssh",
    "-o StrictHostKeyChecking=no",
    "-i",
    "bi-emr2.pem",
    "ec2-user" + "@" + host_ip,
    "cat memberdna/pipelines/trip_spend_model/models_predict.txt ",
]

result = subprocess.check_output(cmd)
print(result)
