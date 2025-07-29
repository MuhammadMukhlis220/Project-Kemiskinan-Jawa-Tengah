## **Project-Kemiskinan-Jawa-Tengah**
---
Data Source:
1. https://jateng.bps.go.id/id/statistics-table/3/UkVkWGJVZFNWakl6VWxKVFQwWjVWeTlSZDNabVFUMDkjMw==/jumlah-dan-persentase-penduduk-miskin-menurut-kabupaten-kota-di-provinsi-jawa-tengah--2023.html?year=2020
2. https://jateng.bps.go.id/id/statistics-table/2/MTg2MiMy/tingkat-pengangguran-terbuka-menurut-kabupaten-kota-dan-jenis-kelamin-di-provinsi-jawa-tengah.html
3. https://jateng.bps.go.id/id/statistics-table/3/TUU0eE4xbE9hMWhSVjNCSGNYaFpWRlJKUjFSTlp6MDkjMw==/persentase-rumah-tangga-yang-memiliki-akses-terhadap-sumber-air-minum-layak-menurut-kabupaten-kota-di-provinsi-jawa-tengah--2021.html
4. https://jateng.bps.go.id/id/statistics-table/2/ODMjMg==/-metode-baru--indeks-pembangunan-manusia-menurut-kabupaten-kota.html

**For ez data, go to [data-source](https://github.com/optimasidata/research-DS/tree/main/poc-dashboard/pemda-kemiskinan/data-source)**

Data flow:
![Alt Text](/pic/workflow.png)

Figure 1

### PostgreSQL

Get all data from [data-source](https://github.com/MuhammadMukhlis220/Project-Kemiskinan-Jawa-Tengah/tree/main/data-source) and export it to postgre. I use import feature from dbeaver like figure 2 bellow:

![Alt Text](/pic/import_postgresql_1.png)

Figure 2

You can follow the next step by using "via csv" to create table. And the result is figure 3:

![Alt Text](/pic/import_postgresql_2.png)

Figure 3

### Airflow - HDFS & Onyx Analytic

After data already in postgre, just upload my [code](ds_pemda_kemiskinan.py) to your Airflow (**remember to change the variable hdfs_node since i am using variable feature**. Look at DAG airflow named `ds_pemda_kesehatan`. There will be 2 tasks, first one is to HDFS (you can create the table using dbeaver too in hive in [here](/data-source/create_hive_for_pemda_kemiskinan.txt). And last one is for OpenSearch's index. **Run the DAG for HDFS's task first. Don't run both of them. It will cause error since the code is created for development by one by one (1 task run 5 times since there are 5 tables). After HDFS's task, run for OpenSearch's task with same pattern.**
I already add some field or column for flagging tag like `Kategori Indeks Pembangunan Manusia` for dashboard purposes that already scripted too in DAG file.

### Onyx Analytic

![Alt Text](/pic/import_opensearch_1.png)

Figure 4

After index pattern created, cook the dashboard. Result in **ds_pemda_kemiskinan**. For snapshot:
<br>
![Alt Text](/pic/dashboard.png)

Figure 5

### Zeppelin

It for analytics purpose and for my notes. Sorry.

<br>
<br>

Give it a try!
