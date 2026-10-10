"""
ingest.py - Giai doan 1: Nap du lieu tho vao Bronze Layer (Chuong 3.2)

Nguon   : diabetes.csv (trong GitHub Repository)
Dich    : MotherDuck -> database DIABETES -> schema bronze -> table diabetes_raw
Nguyen tac: GIU NGUYEN DU LIEU NGUON - khong lam sach, khong ep kieu,
            khong chuyen ky tu '?' thanh NULL (cac buoc do thuoc tang Silver).
"""

import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import duckdb

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

CSV_PATH = os.environ.get("CSV_PATH", "diabetes.csv")
MD_DATABASE = "DIABETES"
BRONZE_TABLE = "bronze.diabetes_raw"
MISSING_COLUMNS = ["race", "weight", "payer_code", "medical_specialty",
                   "diag_1", "diag_2", "diag_3"]


def log(message):
    """Ghi log dong thoi gan thoi diem (phuc vu danh gia tai muc 4.1)."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {message}", flush=True)


def load_raw(path):
    """Doc tệp CSV nguyen ban: moi gia tri deu la chuoi, giu nguyen '?' va chuoi rong."""
    if not os.path.exists(path):
        sys.exit(f"Loi: khong tim thay tệp '{path}' trong thu muc hien tai.")
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
        "missing_question_mark": {c: int((df[c] == "?").sum()) for c in MISSING_COLUMNS},
    }
    return stats


def write_bronze(df, token):
    """Ket noi MotherDuck va ghi nguyen vao tang Bronze."""
    con = duckdb.connect(
        f"motherduck:{MD_DATABASE}",
        config={"motherduck_token": token},
    )
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.register("source_df", df)
    con.execute(f"CREATE OR REPLACE TABLE {BRONZE_TABLE} AS SELECT * FROM source_df")
    con.unregister("source_df")

    table_rows = con.execute(f"SELECT COUNT(*) FROM {BRONZE_TABLE}").fetchone()[0]
    table_cols = len(con.execute(f"DESCRIBE {BRONZE_TABLE}").fetchall())
    weight_missing = con.execute(
        f"SELECT SUM(CASE WHEN weight = '?' THEN 1 ELSE 0 END) FROM {BRONZE_TABLE}"
    ).fetchone()[0]
    con.close()
    return table_rows, table_cols, int(weight_missing)


def main():
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        sys.exit(
            "Loi: thieu bien moi truong MOTHERDUCK_TOKEN.\n"
            "  - tren GitHub Actions: dat token trong Settings > Secrets > MOTHERDUCK_TOKEN\n"
            "  - tren may ca nhan:    set MOTHERDUCK_TOKEN=...  (khong de token vao ma nguon)"
        )

    log("=== BAT DAU NAP DU LIEU THO vao Bronze Layer ===")
    t0 = time.perf_counter()

    df = load_raw(CSV_PATH)
    stats = summarize_raw(df)
    log(f"Doc thanh cong '{CSV_PATH}': {stats['rows']} dong x {stats['cols']} cot")
    log(f"Khoa duy nhat: encounter_id={stats['distinct_encounter_id']}, "
        f"patient_nbr={stats['distinct_patient_nbr']}")
    log(f"Phan phoi readmitted: {stats['readmitted']}")
    log(f"So gia tri '?' (nguyen ban): {stats['missing_question_mark']}")

    log(f"Ket noi MotherDuck '{MD_DATABASE}' ...")
    table_rows, table_cols, weight_missing = write_bronze(df, token)

    elapsed = time.perf_counter() - t0
    log(f"NAP THANH CONG -> {BRONZE_TABLE}: "
        f"{table_rows} dong x {table_cols} cot, thoi gian {elapsed:.1f} giay")
    log(f"Kiem tra giu nguyen du lieu thong: weight='?' = {weight_missing} dong "
        f"(khop voi CSV goc: {stats['missing_question_mark']['weight']})")

    if table_rows != stats["rows"] or table_cols != stats["cols"]:
        sys.exit("Loi: so dong/so cot trong MotherDuck khop voi tệp CSV goc.")

    print()
    print("===== KET QUA PIPELINE (phuc vu danh gia muc 4.1) =====")
    print(f"  Tang                  : BRONZE")
    print(f"  Bang                  : {MD_DATABASE}.{BRONZE_TABLE}")
    print(f"  So dong da nap        : {table_rows}")
    print(f"  So cot da nap         : {table_cols}")
    print(f"  Thoi gian thuc thi    : {elapsed:.1f} giay")
    print(f"  Trang thai            : THANH CONG")
    print("=======================================================")


if __name__ == "__main__":
    main()
