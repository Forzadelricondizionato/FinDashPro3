import ssl
from pathlib import Path
import os
import subprocess

def generate_self_signed_certs():
    cert_dir = Path("/tmp/fdp-certs")
    cert_dir.mkdir(exist_ok=True)
    
    if not (cert_dir / "ca.crt").exists():
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:4096", "-keyout", str(cert_dir / "ca.key"),
            "-out", str(cert_dir / "ca.crt"), "-days", "365", "-nodes",
            "-subj", "/C=US/ST=State/L=City/O=FinDashPro/CN=fdp-ca"
        ], check=True)
    
    for service in ["app", "redis", "postgres"]:
        if not (cert_dir / f"{service}.crt").exists():
            subprocess.run([
                "openssl", "req", "-newkey", "rsa:4096", "-keyout", str(cert_dir / f"{service}.key"),
                "-out", str(cert_dir / f"{service}.csr"), "-nodes",
                "-subj", f"/C=US/ST=State/L=City/O=FinDashPro/CN={service}"
            ], check=True)
            subprocess.run([
                "openssl", "x509", "-req", "-in", str(cert_dir / f"{service}.csr"),
                "-CA", str(cert_dir / "ca.crt"), "-CAkey", str(cert_dir / "ca.key"),
                "-out", str(cert_dir / f"{service}.crt"), "-days", "365", "-CAcreateserial"
            ], check=True)
    
    return cert_dir

def get_ssl_context(server: bool = False, for_client: str = None):
    cert_dir = generate_self_signed_certs()
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH if server else ssl.SERVER_AUTH)
    
    if server:
        context.load_cert_chain(str(cert_dir / "app.crt"), str(cert_dir / "app.key"))
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(str(cert_dir / "ca.crt"))
    else:
        context.load_cert_chain(str(cert_dir / f"{for_client}.crt"), str(cert_dir / f"{for_client}.key"))
        context.check_hostname = True
        context.load_verify_locations(str(cert_dir / "ca.crt"))
    
    return context

