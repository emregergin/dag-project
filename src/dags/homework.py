from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator as EmptyOperator

from pymongo.mongo_client import MongoClient

from datetime import datetime, timedelta
import psycopg2

# DAG tanımı
with DAG(
        dag_id="homework",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        schedule_interval="*/5 * * * *"
) as dag:

    client = MongoClient("mongodb+srv://cetingokhan:cetingokhan@cluster0.ff5aw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")


    def generate_random_heat_and_humidity_data(dummy_record_count: int):
        import random
        import datetime
        from models.heat_and_humidity import HeatAndHumidityMeasureEvent
        records = []
        for i in range(dummy_record_count):
            temperature = random.randint(10, 40)
            humidity = random.randint(10, 100)
            timestamp = datetime.datetime.now()
            creator = "airflow"
            record = HeatAndHumidityMeasureEvent(temperature, humidity, timestamp, creator)
            records.append(record)
        return records


    def save_data_to_mongodb(records):
        db = client["bigdata_training"]
        collection = db["user_coll_emregergin"]
        for record in records:
            collection.insert_one(record.__dict__)


    def create_sample_data_on_mongodb():
        ###her dakika çalışacak ve sonrasında mongodb ye kayıt yapacak method içeriğini tamamlayınız
        # while True:
        records = generate_random_heat_and_humidity_data(10)
        #### eksik parçayı tamamlayınız
        save_data_to_mongodb(records)
        # time.sleep(60)


    def copy_anomalies_into_new_collection():
        # sample_coll collectionundan temperature 30 dan büyük olanları new(kendi adınıza bir collectionname)
        # collectionuna kopyalayın(kendi creatorunuzu ekleyin)
        db = client["bigdata_training"]
        coll1 = db["user_coll_emregergin"]
        coll2 = db["anomalies_emregergin"]

        coll2.delete_many({})

        # Sıcaklık > 30
        anomalous_data = coll1.find({"temperature": {"$gt": 30}})

        for rec in anomalous_data:
            rec.pop("_id", None)
            rec["creator"] = "emregergin"
            coll2.insert_one(rec)


    def copy_airflow_logs_into_new_collection():
        # airflow veritababnındaki log tablosunda bulunan verilerin son 1 dakikasında oluşan event bazındaki kayıt sayısını
        # mongo veritabanında oluşturacağınız"log_adınız" collectionına event adı ve kayıt sayısı bilgisi ile
        # birlikte(güncel tarih alanına ekleyerek) yeni bir tabloya kaydedin.
        # Örn çıktı;
        # {
        #    "event_name": "task_started",
        #    "record_count": 10,
        #    "created_at": "2022-01-01 00:00:00"
        # }

        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="airflow",
            user="airflow",
            password="airflow"
        )

        # Databasede Python kodu çalıştırmak için
        cursor = conn.cursor()

        # Bir dakika öncesi
        one_minute_ago = datetime.now() - timedelta(minutes=1)

        # Bir dakikada oluşan eventlerin sayısı
        query = """
                SELECT event, COUNT(*)
                FROM log
                WHERE log.dttm > %s
                GROUP BY event
                """

        # Sorguyu çalıştır ve kayıtları çek
        cursor.execute(query, (one_minute_ago,))
        records = cursor.fetchall()

        # Veritabanında collection oluştur
        db = client["bigdata_training"]
        collection = db["log_emregergin"]

        # Collection'a logları ekle
        for event, count in records:
            log = {
                "event_name": event,
                "record_count": count,
                "created_at": datetime.now()
            }

            collection.insert_one(log)

        cursor.close()
        conn.close()  # Sürekli kayıt atmaması için Airflow bağlantısını kapat


    dag_start = EmptyOperator(task_id="start", dag=dag)

    create_sample_data = PythonOperator(
        task_id="create_sample_data",
        python_callable=create_sample_data_on_mongodb
    )

    copy_anomalies_into_new_collection = PythonOperator(
        task_id="copy_anomalies_into_new_collection",
        python_callable=copy_anomalies_into_new_collection
    )

    insert_airflow_logs_into_mongodb = PythonOperator(
        task_id="insert_airflow_logs_into_mongodb",
        python_callable=copy_airflow_logs_into_new_collection
    )

    dag_finish = EmptyOperator(task_id="finish", dag=dag)

    dag_start >> create_sample_data >> copy_anomalies_into_new_collection >> dag_finish
    dag_start >> insert_airflow_logs_into_mongodb >> dag_finish