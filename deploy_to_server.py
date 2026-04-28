import paramiko

def run_remote_command(ssh, command, timeout=60):
    print(f"Running: {command}")
    stdin, stdout, stderr = ssh.exec_command(command, timeout=timeout)
    out = stdout.read().decode('utf-8', errors='ignore').strip()
    err = stderr.read().decode('utf-8', errors='ignore').strip()
    if out: 
        try:
            print(f"STDOUT: {out}")
        except UnicodeEncodeError:
            print(f"STDOUT: {out.encode('ascii', errors='ignore').decode()}")
    if err:
        try:
            print(f"STDERR: {err}")
        except UnicodeEncodeError:
            print(f"STDERR: {err.encode('ascii', errors='ignore').decode()}")
    return out, err

def run_background_command(ssh, command):
    """Launch a background process without waiting for output."""
    print(f"Starting in background: {command}")
    transport = ssh.get_transport()
    channel = transport.open_session()
    channel.exec_command(command)
    channel.close()
    print("Background process started.")

def deploy():
    host = "185.2.103.84"
    user = "root"
    pw = "7WSJe676w"
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, username=user, password=pw)
        print("Connected successfully!")
        
        project_path = "/root/TripsHelper"
            
        # 1. Pull changes (force server to match GitHub)
        print("Updating code from GitHub...")
        run_remote_command(ssh, f"cd {project_path} && git fetch origin main && git reset --hard origin/main")
        
        # 2. Setup venv only if missing
        print("Checking venv...")
        run_remote_command(ssh, f"cd {project_path} && [ ! -d venv ] && python3 -m venv venv && ./venv/bin/pip install -r requirements.txt || echo 'venv OK'")

        # 3. Kill old bot
        print("Restarting bot...")
        run_remote_command(ssh, "pkill -9 -f 'bot.py' || true")
        
        # 4. Start bot in background (no wait for output)
        run_background_command(ssh, f"cd {project_path} && nohup ./venv/bin/python bot.py > bot.log 2>&1 &")
        
        print("Deployment finished!")
        
    finally:
        ssh.close()

if __name__ == "__main__":
    deploy()
