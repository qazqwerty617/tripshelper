import paramiko

def check_server():
    host = "185.2.103.84"
    user = "root"
    pw = "7WSJe676w"
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, username=user, password=pw)
        print("Connected successfully!")
        
        project_path = "/root/TripsHelper"
        
        print("\n--- Current Git Commit ---")
        stdin, stdout, stderr = ssh.exec_command(f"cd {project_path} && git log -1 --oneline")
        print(stdout.read().decode())
        
        print("\n--- Restarting Bot ---")
        ssh.exec_command("pkill -9 -f bot.py")
        
        # Start and wait a bit to see if it stays alive
        transport = ssh.get_transport()
        channel = transport.open_session()
        channel.exec_command(f"cd {project_path} && ./venv/bin/python bot.py > bot.log 2>&1 &")
        
        import time
        time.sleep(10) # Wait longer for connection
        
        print("\n--- Bot Processes After 10s ---")
        stdin, stdout, stderr = ssh.exec_command("ps aux | grep bot.py | grep -v grep")
        print(stdout.read().decode())
        
        print("\n--- Last 30 lines of bot.log ---")
        stdin, stdout, stderr = ssh.exec_command(f"cd {project_path} && tail -n 30 bot.log")
        print(stdout.read().decode())
        
    finally:
        ssh.close()

if __name__ == "__main__":
    check_server()
