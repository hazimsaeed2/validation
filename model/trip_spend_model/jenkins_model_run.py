import argparse
import os
import subprocess
import time

import boto3
import paramiko


def execute_ssh_command(ssh_key_path, host, username, command):
    # k = paramiko.RSAKey.from_private_key_file(ssh_key_path)
    for i in range(0, 10):
        print(f"SSH command execution: iteration {i+1}")
        cmd = [
            "ssh",
            "-o StrictHostKeyChecking=no",
            "-i",
            "bi-emr2.pem",
            "ec2-user" + "@" + host_ip,
            command,
        ]

        print(f"Executing command {command}")
        try:
            result = subprocess.check_output(cmd)
            print("output: ", result)
            break
        except subprocess.CalledProcessError as e:
            if i == 9:
                raise Exception("error when running " + str(cmd))
            else:
                print("error when running " + str(cmd) + ":" + str(e.output))

        # print(f"SSH command execution: iteration {i+1}")
        # c = paramiko.SSHClient()
        # c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # print(f"Trying to connect to TP Model EC2 machine: {host} ")
        # c.connect(hostname=host, username=username, pkey=k)
        # print(f"Connected to TP Model EC2 machine: {host} ")
        # print(f"Executing command {command}")
        # _, stdout, stderr = c.exec_command(command)
        # exit_code = stdout.channel.recv_exit_status()
        # stderr_str = stderr.read()
        # stdout_str = stdout.read()
        # print(f"stdout is {stdout_str}")
        # print(f"stderr is {stderr_str}")
        # print(f"exit code is {exit_code}")
        # if exit_code == 0:
        #     break
    # c.close()


def ping_test(host):
    if (os.system("ping -c 1 " + host)) == 0:
        print("Successfully pinged")
        return 1
    else:
        return 0


parser = argparse.ArgumentParser(description="Model run.")
parser.add_argument("config_path", action="store", help="Config path")
parser.add_argument(
    "run_type", action="store", help="The run type for this execution"
)
parser.add_argument("git_branch", action="store", help="Git branch")
args = parser.parse_args()

config_path = args.config_path
run_type = args.run_type
git_branch = ("branch/" + args.git_branch.replace("origin/", "")).replace(
    "/", "_"
)

config_suffix = ""

if run_type == "dev" or run_type == "stage":
    config_suffix = f"_{run_type}"

yaml_path = f"./conf/config{config_suffix}.yml"

ssh_cert = os.environ["ssh_key_file"]
ssh_username = os.environ["ssh_username"]

if not os.path.isfile("bi-emr2.pem"):
    with open(ssh_cert, "r") as myfile:
        rsa_contents = myfile.read()

    with open("bi-emr2.pem", "w") as myfile:
        myfile.write(rsa_contents)

ec2 = boto3.client("ec2")

tag_name = "ue00mdaapp01"
if run_type in ["dev", "stage"]:
    tag_name = f"{tag_name}_{run_type}_{git_branch}"
elif run_type == "prod":
    tag_name = f"{tag_name}_{run_type}"

response = ec2.describe_instances(
    Filters=[{"Name": "tag:Hostname", "Values": [tag_name]}]
)

host_ip = None

for rsvn in response.get("Reservations"):
    if "PrivateIpAddress" in rsvn["Instances"][0]:
        host_ip = rsvn["Instances"][0]["PrivateIpAddress"]
        break

if host_ip is None:
    raise Exception("Cannot get the host IP of the EC2 instance")

print("Using EC2 IpAddress of " + host_ip)

subprocess.check_call(["chmod", "400", "bi-emr2.pem"])

# initialized a counter.
n = 0
# runs until n < 24,just to avoid the infinite loop.
# this will execute the ping_test() func in every 10 seconds.
while n < 24:
    ping_return = ping_test(host_ip)

    if ping_return == 1:
        break
    else:
        time.sleep(10)
        n = n + 1

command_list = [
    "sudo yum install git -y",
    "sudo yum install python3-pip -y",
    "python3 -m venv myenv",
    "source myenv/bin/activate && python3 -m pip install --index-url=https://nexus.bjs.com/repository/pypi-group/simple/ --trusted-host nexus.bjs.com pyarrow==8.0.0",
    "source myenv/bin/activate && python3 -m pip install --index-url=https://nexus.bjs.com/repository/pypi-group/simple/ --trusted-host nexus.bjs.com joblib==1.1.0",
    "source myenv/bin/activate && python3 -m pip install --index-url=https://nexus.bjs.com/repository/pypi-group/simple/ --trusted-host nexus.bjs.com boto3==1.26.5",
    "source myenv/bin/activate && python3 -m pip install --index-url=https://nexus.bjs.com/repository/pypi-group/simple/ --trusted-host nexus.bjs.com numpy==1.21.6",
    "source myenv/bin/activate && python3 -m pip install --index-url=https://nexus.bjs.com/repository/pypi-group/simple/ --trusted-host nexus.bjs.com s3io==0.1.1",
    "source myenv/bin/activate && python3 -m pip install --index-url=https://nexus.bjs.com/repository/pypi-group/simple/ --trusted-host nexus.bjs.com pyaml==21.10.1",
    "source myenv/bin/activate && python3 -m pip install --index-url=https://nexus.bjs.com/repository/pypi-group/simple/ --trusted-host nexus.bjs.com pandas==1.3.5",
    "source myenv/bin/activate && python3 -m pip install --index-url=https://nexus.bjs.com/repository/pypi-group/simple/ --trusted-host nexus.bjs.com scikit-learn==0.24.2",
]

if ping_test(host_ip):
    print("Executing new command")
    for ssh_command in command_list:
        execute_ssh_command("bi-emr2.pem", host_ip, "ec2-user", ssh_command)
else:
    print("Ping test failed")
    raise Exception(f"EC2 machine {host_ip} is not reachable ")

scp_command = [
    "scp",
    "-v",
    "-i",
    "bi-emr2.pem",
    "-o StrictHostKeyChecking=no",
    "-r",
    "pe_memberdna/",
    "ec2-user" + "@" + host_ip + ":pe_memberdna/",
]

try:
    result = subprocess.run(scp_command, capture_output=True, text=True)
except subprocess.CalledProcessError as e:
    raise RuntimeError(
        "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, e.output
        )
    )

ssh_command = f"cd pe_memberdna/model/trip_spend_model ; export PYTHONPATH=/home/ec2-user ; make models_predict config={yaml_path} > models_predict.txt "

execute_ssh_command("bi-emr2.pem", host_ip, "ec2-user", ssh_command)

ssh_command = "cat pe_memberdna/model/trip_spend_model/models_predict.txt"

execute_ssh_command("bi-emr2.pem", host_ip, "ec2-user", ssh_command)
