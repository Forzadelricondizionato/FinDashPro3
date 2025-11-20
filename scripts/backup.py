# scripts/backup.py
import subprocess
import datetime
import os
import gzip
import boto3
from pathlib import Path
import structlog

logger = structlog.get_logger()

def backup_postgres() -> Path:
    try:
        today = datetime.date.today().strftime("%Y%m%d")
        backup_dir = Path(os.getenv("BACKUP_DIR", "/opt/findashpro/backups"))
        backup_dir.mkdir(exist_ok=True, parents=True)
        
        dump_file = backup_dir / f"fdp_{today}.sql"
        gz_file = backup_dir / f"fdp_{today}.sql.gz"
        
        result = subprocess.run([
            "pg_dump", "-h", os.getenv("DB_HOST", "localhost"), 
            "-U", os.getenv("DB_USER", "fdp"), 
            "-d", os.getenv("DB_NAME", "findashpro"), 
            "-f", str(dump_file)
        ], env={"PGPASSWORD": os.getenv("DB_PASS")}, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error("postgres_backup_failed", stderr=result.stderr)
            return None
        
        with open(dump_file, 'rb') as f_in:
            with gzip.open(gz_file, 'wb') as f_out:
                f_out.writelines(f_in)
        
        dump_file.unlink()
        logger.info("postgres_backup_success", file=str(gz_file))
        return gz_file
        
    except Exception as e:
        logger.error("postgres_backup_error", error=str(e))
        return None

def backup_redis() -> Path:
    try:
        today = datetime.date.today().strftime("%Y%m%d")
        backup_dir = Path(os.getenv("BACKUP_DIR", "/opt/findashpro/backups"))
        backup_dir.mkdir(exist_ok=True, parents=True)
        
        rdb_file = backup_dir / f"redis_{today}.rdb"
        subprocess.run(["redis-cli", "bgsave"], check=True)
        subprocess.run(["cp", "/var/lib/redis/dump.rdb", str(rdb_file)], check=True)
        
        logger.info("redis_backup_success", file=str(rdb_file))
        return rdb_file
        
    except Exception as e:
        logger.error("redis_backup_error", error=str(e))
        return None

def upload_to_b2(file_path: Path, bucket_name: str):
    try:
        b2 = boto3.client(
            service_name='s3',
            endpoint_url='https://s3.eu-central-003.backblazeb2.com',
            aws_access_key_id=os.getenv('B2_KEY_ID'),
            aws_secret_access_key=os.getenv('B2_APP_KEY')
        )
        b2.upload_file(str(file_path), bucket_name, file_path.name)
        logger.info("b2_upload_success", file=file_path.name)
    except Exception as e:
        logger.error("b2_upload_error", error=str(e))

if __name__ == "__main__":
    pg_backup = backup_postgres()
    redis_backup = backup_redis()
    
    if pg_backup:
        upload_to_b2(pg_backup, "fdp-backups")
    if redis_backup:
        upload_to_b2(redis_backup, "fdp-backups")
