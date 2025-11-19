# scripts/backup.py
import subprocess
import datetime
import os
import gzip
import boto3
from pathlib import Path

def backup_postgres():
    today = datetime.date.today().strftime("%Y%m%d")
    backup_dir = Path("/opt/findashpro/backups")
    backup_dir.mkdir(exist_ok=True)
    
    dump_file = backup_dir / f"fdp_{today}.sql"
    gz_file = backup_dir / f"fdp_{today}.sql.gz"
    
    subprocess.run([
        "pg_dump", "-h", "localhost", "-U", "fdp", "findashpro", "-f", str(dump_file)
    ], env={"PGPASSWORD": os.getenv("DB_PASS")})
    
    with open(dump_file, 'rb') as f_in:
        with gzip.open(gz_file, 'wb') as f_out:
            f_out.writelines(f_in)
    
    dump_file.unlink()
    return gz_file

def backup_redis():
    today = datetime.date.today().strftime("%Y%m%d")
    backup_dir = Path("/opt/findashpro/backups")
    backup_dir.mkdir(exist_ok=True)
    
    rdb_file = backup_dir / f"redis_{today}.rdb"
    subprocess.run(["redis-cli", "bgsave"])
    subprocess.run(["cp", "/var/lib/redis/dump.rdb", str(rdb_file)])
    
    return rdb_file

def upload_to_b2(file_path: Path, bucket_name: str):
    b2 = boto3.client(
        service_name='s3',
        endpoint_url='https://s3.eu-central-003.backblazeb2.com',
        aws_access_key_id=os.getenv('B2_KEY_ID'),
        aws_secret_access_key=os.getenv('B2_APP_KEY')
    )
    
    b2.upload_file(str(file_path), bucket_name, file_path.name)

if __name__ == "__main__":
    pg_backup = backup_postgres()
    redis_backup = backup_redis()
    upload_to_b2(pg_backup, "fdp-backups")
    upload_to_b2(redis_backup, "fdp-backups")
