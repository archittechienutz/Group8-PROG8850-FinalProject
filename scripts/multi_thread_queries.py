import os, threading, random
import mysql.connector as mysql

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_USER = os.getenv("DB_ADMIN_USER", "root")
DB_PASS = os.getenv("DB_PASSWORD", "Secret5555")  # change if different
DB_NAME = os.getenv("DB_NAME", "project_db")

def get_conn():
    return mysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)

def insert_rows(n=20):
    with get_conn() as c:
        cur = c.cursor()
        cities = ["Toronto","Waterloo","Ottawa","Hamilton"]
        for _ in range(n):
            cur.execute(
                """INSERT INTO ClimateData(location,record_date,temperature,precipitation,humidity)
                   VALUES(%s, CURDATE(), %s, %s, %s)""",
                (
                    random.choice(cities),
                    round(random.uniform(15,35),1),
                    round(random.uniform(0,10),1),
                    round(random.uniform(30,90),1),
                ),
            )
        c.commit()

def select_hot():
    with get_conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM ClimateData WHERE temperature > 20")
        print("Rows > 20°C:", cur.fetchone()[0])

def update_humidity():
    with get_conn() as c:
        cur = c.cursor()
        cur.execute("UPDATE ClimateData SET humidity = humidity + 1 WHERE location='Toronto'")
        print("Updated (Toronto humidity +1):", cur.rowcount)
        c.commit()

if __name__ == "__main__":
    threads = [
        threading.Thread(target=insert_rows, args=(20,)),
        threading.Thread(target=select_hot),
        threading.Thread(target=update_humidity),
    ]
    [t.start() for t in threads]
    [t.join() for t in threads]
    print("Concurrent ops complete.")
