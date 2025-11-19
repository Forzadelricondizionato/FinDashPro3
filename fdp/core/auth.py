# fdp/core/auth.py
import pyotp
import qrcode
import io
import base64
from typing import Optional

class TOTPAuthenticator:
    def __init__(self, secret: Optional[str] = None):
        self.secret = secret or pyotp.random_base32()
        self.totp = pyotp.TOTP(self.secret)
    
    def get_qr_code(self, account_name: str) -> str:
        uri = self.totp.provisioning_uri(name=account_name, issuer_name="FinDashPro")
        qr = qrcode.make(uri)
        buffer = io.BytesIO()
        qr.save(buffer, format="PNG")
        return base64.b64encode(buffer.getvalue()).decode()
    
    def verify(self, code: str) -> bool:
        return self.totp.verify(code)
    
    def get_current_code(self) -> str:
        return self.totp.now()
