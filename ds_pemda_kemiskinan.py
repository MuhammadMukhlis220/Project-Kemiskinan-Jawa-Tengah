from airflow import DAG
from airflow.decorators import task
from datetime import datetime, date
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils import timezone
from airflow.models import Variable
from sqlalchemy import create_engine
import psycopg2
from opensearchpy.helpers import bulk
from opensearchpy import OpenSearch
import pandas as pd
import uuid
from hdfs import InsecureClient
import os
import pyarrow as pa
import pyarrow.orc as orc
import io

hdfs_node = Variable.get("hdfs_poc")
hdfs_client = InsecureClient(f'http://{hdfs_node}:50070', user='airflow')

dir_hdfs = "/hive_external_data/ds_pemda/kemiskinan/jawa_tengah"
folder = [
    "pengangguran",
    "akses_air_minum_layak",
    "indeks_pembangunan_manusia"
        ]

table_postgre = [
    "pengangguran_jateng",
    "akses_air_minum_layak_jateng",
    "indeks_pembangunan_manusia_jateng"
        ]

def postgre_to_hive():

    engine = create_engine('postgresql+psycopg2://x:x@x.x.x.x/x')
    
    for folder_name, table_name in zip(folder, table_postgre):
        dir_hdfs_folder = f"{dir_hdfs}/{folder_name}"
        
        # HDFS FOLDER
        if hdfs_client.status(dir_hdfs_folder, strict=False) is not None:
            hdfs_client.delete(dir_hdfs_folder, recursive=True)
            print(f"Hapus folder: {dir_hdfs_folder}")

        hdfs_client.makedirs(dir_hdfs_folder)
        print(f"Buat folder: {dir_hdfs_folder}")

        os.system(f'hdfs dfs -chmod 777 {dir_hdfs_folder}')
        print(f"Set permissions to 777 for {dir_hdfs_folder}")

        # AMIBL DATA
        with engine.connect() as conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, con=conn)

        df['execute_time'] = date.today()
        df.columns = [col.strip().lower().replace(" ", "_").replace(".","").replace("(", "").replace(")", "").replace("/", "_").replace("-", "_") for col in df.columns]
        
        # ORC
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        with orc.ORCWriter(buffer) as writer:
            writer.write(table)

        orc_data = buffer.getvalue()

        # Simpan ke HDFS
        orc_path = f"{dir_hdfs_folder}/{folder_name}.orc"
        with hdfs_client.write(orc_path, overwrite=True) as writer:
            writer.write(orc_data)

        print(f"✅ Berhasil write ORC ke HDFS: {orc_path}")
    

def postgre_to_opensearch():
    
    engine = create_engine('postgresql+psycopg2://x:x@x.x.x.x/x')
    conn = None

    try:
        conn = engine.raw_connection()
        conn.autocommit = True
        
    # MOdify persen pengangguran by indeks
        '''
        query = """
        SELECT
    CASE
        WHEN a."Wilayah" IN ('Kabupaten Banjarnegara', 'Kabupaten Wonosobo', 'Kabupaten Pemalang', 'Kabupaten Brebes') THEN 'Wilayah Indeks Rendah'
        WHEN a."Wilayah" IN ('Kota Magelang', 'Kota Surakarta', 'Kota Salatiga', 'Kota Semarang') THEN 'Wilayah Indeks Tinggi'
        ELSE 'Wilayah Indeks Menengah'
    END AS "Kategori Indeks Pembangunan Manusia",
    a."Tahun",
    ROUND(
        cast(SUM(a."Total" * b."Jumlah Penduduk Ribu Jiwa" / 100.0) / SUM(b."Jumlah Penduduk Ribu Jiwa") * 100.0 as numeric)
    , 2) AS "Persentase Pengangguran"
FROM public.pengangguran_jateng a
LEFT JOIN public.penduduk_miskin_jateng b
    ON a."Wilayah" = b."Wilayah" AND a."Tahun" = b."Tahun"
GROUP BY "Kategori Indeks Pembangunan Manusia", a."Tahun"
ORDER BY a."Tahun", "Kategori Indeks Pembangunan Manusia";
        """
        '''
    # Modify persen kemiskinan by indeks
    
        '''
        query = """
        SELECT 
            CASE
                WHEN "Wilayah" IN ('Kabupaten Banjarnegara', 'Kabupaten Wonosobo', 'Kabupaten Pemalang', 'Kabupaten Brebes') THEN 'Wilayah Indeks Rendah'
                WHEN "Wilayah" IN ('Kota Magelang', 'Kota Surakarta', 'Kota Salatiga', 'Kota Semarang') THEN 'Wilayah Indeks Tinggi'
                ELSE 'Wilayah Indeks Menengah'
            END AS "Kategori Indeks Pembangunan Manusia",
            "Tahun",
            ROUND(
                CAST(SUM("Jumlah Penduduk Miskin Maret Ribu Jiwa") * 100.0 / SUM("Jumlah Penduduk Ribu Jiwa") AS numeric),
                2
            ) AS "Persentase Kemiskinan"
        FROM 
            penduduk_miskin_jateng
        WHERE 
            "Tahun" BETWEEN 2020 AND 2024
        GROUP BY 
            "Kategori Indeks Pembangunan Manusia", "Tahun"
        ORDER BY 
            "Tahun", "Kategori Indeks Pembangunan Manusia"
        """
        '''
        
    # Modify Air Minum Layak
    
        '''
        query = """
        SELECT  
            n."Wilayah", 
            "Tahun", 
            "Persentase Akses Rumah ke Air Minum yang Layak",
            CASE
                WHEN n."Wilayah" IN ('Kabupaten Banjarnegara', 'Kabupaten Wonosobo', 'Kabupaten Pemalang', 'Kabupaten Brebes') THEN 'Wilayah Indeks Rendah'
                WHEN n."Wilayah" IN ('Kota Magelang', 'Kota Surakarta', 'Kota Salatiga', 'Kota Semarang') THEN 'Wilayah Indeks Tinggi'
                ELSE 'Wilayah Indeks Menengah'
                END AS "Kategori Indeks Pembangunan Manusia",
             "Lat",
             "Long"
        FROM 
            public.akses_air_minum_layak_jateng n
        LEFT JOIN 
            public.lokasi_jawa_tengah s
          ON n."Wilayah" = s."Wilayah"
        """
        '''
        
    # Modify Indeks Pembangunan Manusia
        
        '''
        query = """
        SELECT  
               n."Wilayah", 
               "Tahun", 
               "Usia Harapan Hidup saat Lahir (tahun)", 
               "Harapan Lama Sekolah (tahun)", 
               "Rata-rata Lama Sekolah (tahun)", 
               "Pengeluaran per kapita (ribu rupiah/orang/tahun)", 
               "Indeks Pembangunan Manusia",
                CASE
                    WHEN n."Wilayah" IN ('Kabupaten Banjarnegara', 'Kabupaten Wonosobo', 'Kabupaten Pemalang', 'Kabupaten Brebes') THEN 'Wilayah Indeks Rendah'
                    WHEN n."Wilayah" IN ('Kota Magelang', 'Kota Surakarta', 'Kota Salatiga', 'Kota Semarang') THEN 'Wilayah Indeks Tinggi'
                    ELSE 'Wilayah Indeks Menengah'
                END AS "Kategori Indeks Pembangunan Manusia",
               "Lat",
               "Long"
        FROM 
            public.indeks_pembangunan_manusia_jateng n
        LEFT JOIN 
            public.lokasi_jawa_tengah s
          ON n."Wilayah" = s."Wilayah"
        """
        '''
        
    # Modify Pengangguran Persentase
        
        '''
        query = """
        SELECT  
              n.*,
              m."Jumlah Penduduk Ribu Jiwa",
              n."Total" * m."Jumlah Penduduk Ribu Jiwa" / 100 AS "Jumlah Pengangguran Ribu Jiwa",
              CASE
                   WHEN n."Wilayah" IN ('Kabupaten Banjarnegara', 'Kabupaten Wonosobo', 'Kabupaten Pemalang', 'Kabupaten Brebes') THEN 'Wilayah Indeks Rendah'
                   WHEN n."Wilayah" IN ('Kota Magelang', 'Kota Surakarta', 'Kota Salatiga', 'Kota Semarang') THEN 'Wilayah Indeks Tinggi'
                   ELSE 'Wilayah Indeks Menengah'
              END AS "Kategori Indeks Pembangunan Manusia",
              "Lat",
              "Long"
        FROM 
            public.pengangguran_jateng n
        LEFT JOIN 
            public.lokasi_jawa_tengah s
            ON n."Wilayah" = s."Wilayah"
        LEFT JOIN
            public.penduduk_miskin_jateng m
            ON n."Wilayah" = m."Wilayah" AND n."Tahun" = m."Tahun"
         """
         '''
        
    # Modify BANSOS KORUP
        
        query = """
        WITH stats AS (
            SELECT
                MIN(n."Total") AS min_pengangguran,
                MAX(n."Total") AS max_pengangguran,
                MIN(m."Persentase Penduduk Miskin Maret") AS min_miskin,
                MAX(m."Persentase Penduduk Miskin Maret") AS max_miskin,
                MIN(o."Persentase Akses Rumah ke Air Minum yang Layak") AS min_air,
                MAX(o."Persentase Akses Rumah ke Air Minum yang Layak") AS max_air
            FROM 
                public.pengangguran_jateng n
            LEFT JOIN 
                public.penduduk_miskin_jateng m
                ON n."Wilayah" = m."Wilayah" AND n."Tahun" = m."Tahun"
            LEFT JOIN
                public.akses_air_minum_layak_jateng o
                ON n."Wilayah" = o."Wilayah" AND n."Tahun" = o."Tahun"
        ),
        merged AS (
            SELECT  
                n."Wilayah",
                n."Tahun",
                n."Total" AS "Persentase Pengangguran",
                m."Jumlah Penduduk Ribu Jiwa",
                n."Total" * m."Jumlah Penduduk Ribu Jiwa" / 100 AS "Jumlah Pengangguran Ribu Jiwa",
                m."Persentase Penduduk Miskin Maret" AS "Persentase Penduduk Miskin",
                CASE
                    WHEN n."Wilayah" IN ('Kabupaten Banjarnegara', 'Kabupaten Wonosobo', 'Kabupaten Pemalang', 'Kabupaten Brebes') THEN 'Wilayah Indeks Rendah'
                    WHEN n."Wilayah" IN ('Kota Magelang', 'Kota Surakarta', 'Kota Salatiga', 'Kota Semarang') THEN 'Wilayah Indeks Tinggi'
                    ELSE 'Wilayah Indeks Menengah'
                END AS "Kategori Indeks Pembangunan Manusia",
                o."Persentase Akses Rumah ke Air Minum yang Layak",
                s."Lat",
                s."Long"
            FROM 
                public.pengangguran_jateng n
            LEFT JOIN 
                public.lokasi_jawa_tengah s
                ON n."Wilayah" = s."Wilayah"
            LEFT JOIN
                public.penduduk_miskin_jateng m
                ON n."Wilayah" = m."Wilayah" AND n."Tahun" = m."Tahun"
            LEFT JOIN
                public.akses_air_minum_layak_jateng o
                ON n."Wilayah" = o."Wilayah" AND n."Tahun" = o."Tahun"
        )
        SELECT 
            m.*,
            ROUND(((m."Persentase Pengangguran" - s.min_pengangguran) / NULLIF(s.max_pengangguran - s.min_pengangguran, 0))::numeric, 3) AS pengangguran_norm,
            ROUND(((m."Persentase Penduduk Miskin" - s.min_miskin) / NULLIF(s.max_miskin - s.min_miskin, 0))::numeric, 3) AS miskin_norm,
            ROUND(((s.max_air - m."Persentase Akses Rumah ke Air Minum yang Layak") / NULLIF(s.max_air - s.min_air, 0))::numeric, 3) AS air_norm,
            ROUND((
                0.2 * ((m."Persentase Pengangguran" - s.min_pengangguran) / NULLIF(s.max_pengangguran - s.min_pengangguran, 0)) +
                0.3 * ((m."Persentase Penduduk Miskin" - s.min_miskin) / NULLIF(s.max_miskin - s.min_miskin, 0)) +
                0.1 * ((s.max_air - m."Persentase Akses Rumah ke Air Minum yang Layak") / NULLIF(s.max_air - s.min_air, 0))
            )::numeric, 3) AS "Skor Prioritas"
        FROM merged m
        CROSS JOIN stats s;
         """
        
        
        df = pd.read_sql(query, con=conn)
        
        
        df["Location"] = df.apply(lambda row: {"lat": row["Lat"], "lon": row["Long"]}, axis=1)
        df.drop(columns=["Lat", "Long"], inplace=True)    
        
        
        df['execute_time'] = date.today()

    finally:
        if conn is not None:
            conn.close()
            
    OPENSEARCH_HOST = "x.x.x.x"
    OPENSEARCH_PORT = "9220"
    OPENSEARCH_INDEX = "jateng_bansos_klasifikasi"
    OPENSEARCH_TYPE = "_doc"
    OPENSEARCH_URL = "https://x:x@x.x.x.x:9220/"
    OPENSEARCH_CLUSTER = "ONYX-analytic"
    ONYX_OS = OpenSearch(
                         hosts = [{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}], http_auth = ("admin", "admin"),
                         use_ssl = True, verify_certs = False, ssl_assert_hostname = False, ssl_show_warn = False
                        )
    batch = 100000
    

    for x in range(0, len(df), batch):
        df = df.iloc[x:x+batch]
        hits = [{"_op_type": "index", "_index": OPENSEARCH_INDEX, "_id": str(uuid.uuid4()), "_score": 1, "_source": i} for i in df.to_dict("records")]
        resp, err = bulk(ONYX_OS, hits, index=OPENSEARCH_INDEX, max_retries=3)
        print(resp, err)

with DAG(
    'ds_pemda_kemiskinan',
    default_args={
        'start_date': datetime(2025, 5, 1)
    },
    schedule_interval=None,
    catchup=False,
    tags=['dev', 'kesehatan']
) as dag:
    '''
    t1 = PythonOperator(
        task_id='postgre_to_hive',
        python_callable=postgre_to_hive,
    )
    '''
    t2 = PythonOperator(
        task_id='postgre_to_opensearch',
        python_callable=postgre_to_opensearch,
    )
    
    
#t1 >> t2
