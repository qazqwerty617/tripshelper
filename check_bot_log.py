import paramiko

def check_log():
    host = "185.2.103.84"
    user = "root"
    pw = "7WSJe676w"
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host, username=user, password=pw)
        stdin, stdout, stderr = ssh.exec_command("tail -n 50 /root/TripsHelper/bot.log")
        print(stdout.read().decode('utf-8', errors='ignore'))
    finally:
        ssh.close()

if __name__ == "__main__":
    check_log()
