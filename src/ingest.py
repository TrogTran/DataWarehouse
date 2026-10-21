"""
ingest.py - Giai doan 1: Nap du lieu tho vao Bronze Layer (Chuong 3.2)

Nguon   : diabetes.csv (trong GitHub Repository)
Dich    : Google Cloud BigQuery -> project diabetes-pipeline-511509 -> dataset bronze -> table diabetes_raw
Nguyen tac: GIU NGUYEN DU LIEU NGUON - khong lam sach, khong ep kieu,
            khong chuyen ky tu '?' thanh NULL (cac buoc do thuoc tang Silver).
"""

import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Cau hinh hien thi tieng Viet tren Windows console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Cac hang so cau hinh
CSV_PATH = os.environ.get("CSV_PATH", "data/diabetes.csv")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "diabetes-pipeline-511509")
DATASET_ID = "bronze"
TABLE_ID = "diabetes_raw"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

MISSING_COLUMNS = ["race", "weight", "payer_code", "medical_specialty",
                   "diag_1", "diag_2", "diag_3"]


def log(message):
    """Ghi log dong thoi gan thoi diem (phuc vu danh gia tai muc 4.1)."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {message}", flush=True)


def load_raw(path):
    """Doc tep CSV nguyen ban: moi gia tri deu la chuoi, giu nguyen '?' va chuoi rong."""
    if not os.path.exists(path):
        sys.exit(f"Loi: khong tim thay tep '{path}' trong thu muc hien tai.")
    # dtype=str: giu nguyen dang chuoi
    # keep_default_na=False: khong tu dong bien chuoi rong thanh NaN
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    return df


def summarize_raw(df):
    """Thong ke du lieu thong de ghi log va doi chieu voi chuong 2 (khong sua gi du lieu)."""
    stats = {
        "rows": len(df),
        "cols": len(df.columns),
        "distinct_encounter_id": df["encounter_id"].nunique(),
        "distinct_patient_nbr": df["patient_nbr"].nunique(),
        "gender_invalid": int((~df["gender"].isin(["Male", "Female"])).sum()),
        "readmitted": df["readmitted"].value_counts().to_dict(),
        "missing_question_mark": {c: int((df[c] == "?").sum()) for c in MISSING_COLUMNS if c in df.columns},
    }
    return stats


def write_bronze(df, project_id):
    """Ket noi Google BigQuery va ghi nguyen vao tang Bronze."""
    client = bigquery.Client(project=project_id)
    
    # 1. Dam bao Dataset 'bronze' ton tai
    dataset_ref = f"{project_id}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        log(f"Dataset '{DATASET_ID}' chua ton tai. Dang tu dong tao moi...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US" # Hoac "asia-southeast1" tuy khu vuc ban chon
        client.create_dataset(dataset)
        log(f"Da tao thanh cong Dataset '{DATASET_ID}'.")

    # 2. Cau hinh nap du lieu
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Ghi de neu bang da ton tai
        autodetect=True, # BigQuery tu dong nhan dien kieu du lieu (chuoi)
    )

    # 3. Thuc hien nap du lieu
    log(f"Dang nap du lieu vao {FULL_TABLE_ID} ...")
    job = client.load_table_from_dataframe(df, FULL_TABLE_ID, job_config=job_config)
    job.result() # Cho den khi job nap hoan tat

    # 4. Kiem tra ket qua sau khi nap
    table = client.get_table(FULL_TABLE_ID)
    table_rows = table.num_rows
    table_cols = len(table.schema)
    
    # 5. Truy van kiem tra tinh toan ven (vi du: dem so luong '?')
    query = f"SELECT COUNT(*) as missing_count FROM `{FULL_TABLE_ID}` WHERE weight = '?'"
    query_job = client.query(query)
    weight_missing = list(query_job.result())[0][0]

    client.close()
    return table_rows, table_cols, int(weight_missing)


def main():
    # Xac thuc: Trong GitHub Actions, file JSON da duoc tu dong nap qua action auth.
    # Khi chay local, ban can set bien moi truong GOOGLE_APPLICATION_CREDENTIALS
    if not os.environ.get("GCP_PROJECT_ID") and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        log("CANH BAO: Khong tim thay thong tin cau hinh GCP. Se dung Project ID mac dinh.")

    log("=== BAT DAU NAP DU LIEU THO vao Bronze Layer (Google BigQuery) ===")
    t0 = time.perf_counter()

    # Doc du lieu
    df = load_raw(CSV_PATH)
    stats = summarize_raw(df)
    log(f"Doc thanh cong '{CSV_PATH}': {stats['rows']} dong x {stats['cols']} cot")
    log(f"Khoa duy nhat: encounter_id={stats['distinct_encounter_id']}, "
        f"patient_nbr={stats['distinct_patient_nbr']}")
    log(f"Phan phoi readmitted: {stats['readmitted']}")
    log(f"So gia tri '?' (nguyen ban): {stats['missing_question_mark']}")

    # Ghi du lieu
    log(f"Ket noi toi Google BigQuery '{PROJECT_ID}' ...")
    table_rows, table_cols, weight_missing = write_bronze(df, PROJECT_ID)

    # Danh gia hieu nang
    elapsed = time.perf_counter() - t0
    log(f"NAP THANH CONG -> {FULL_TABLE_ID}: "
        f"{table_rows} dong x {table_cols} cot, thoi gian {elapsed:.1f} giay")
    log(f"Kiem tra giu nguyen du lieu thong: weight='?' = {weight_missing} dong "
        f"(khop voi CSV goc: {stats['missing_question_mark'].get('weight', 0)})")

    # Kiem tra tinh toan ven
    if table_rows != stats["rows"] or table_cols != stats["cols"]:
        sys.exit("Loi: so dong/so cot trong BigQuery KHONG khop voi tep CSV goc.")

    print()
    print("===== KET QUA PIPELINE (phuc vu danh gia muc 4.1) =====")
    print(f"  Tang                  : BRONZE")
    print(f"  Bang                  : {FULL_TABLE_ID}")
    print(f"  So dong da nap        : {table_rows}")
    print(f"  So cot da nap         : {table_cols}")
    print(f"  Thoi gian thuc thi    : {elapsed:.1f} giay")
    print(f"  Trang thai            : THANH CONG")
    print("=======================================================")


if __name__ == "__main__":
    main()
