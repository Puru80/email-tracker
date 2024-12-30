import json

class Utils: 
    
    def __init__(self, config_file):
        with open(config_file, "r") as f:
            config = json.load(f)

        self.read_ignore_ips = config["read_ignore_ips"]

    def is_ip_ignored(self, client_ip):
        if client_ip in self.read_ignore_ips:
            print("Ignoring IP: ", client_ip)
            return True
        
        return False