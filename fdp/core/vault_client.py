import hvac
import os
import structlog

logger = structlog.get_logger()

class VaultClient:
    def __init__(self):
        self.client = hvac.Client(
            url=os.getenv("VAULT_ADDR", "http://localhost:8200"),
            token=os.getenv("VAULT_TOKEN", "fdp-root-token")
        )
    
    def is_initialized(self):
        return self.client.sys.is_initialized()
    
    def is_authenticated(self):
        return self.client.is_authenticated()
    
    def write_secret(self, path, data):
        if not self.is_authenticated():
            logger.error("Vault not authenticated")
            return False
        try:
            self.client.secrets.kv.v2.create_or_update_secret(path=path, secret=data)
            logger.info("Secret written", path=path)
            return True
        except Exception as e:
            logger.error("Vault write failed", error=str(e))
            return False
    
    def read_secret(self, path):
        if not self.is_authenticated():
            logger.error("Vault not authenticated")
            return None
        try:
            response = self.client.secrets.kv.v2.read_secret_version(path=path)
            return response["data"]["data"]
        except Exception as e:
            logger.error("Vault read failed", path=path, error=str(e))
            return None

