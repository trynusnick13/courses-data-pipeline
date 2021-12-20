import psycopg2

from google.cloud import storage
import csv

storage_client = storage.Client()
bucket = storage_client.get_bucket("courses_ingestion")
blob = bucket.blob("stepik/stepik.csv")
blob.download_to_filename("stepik.csv")

conn = psycopg2.connect(f"postgresql://rkvxqcpddhclhs:14726f38b69d3a9a952901f345a3f0eda56d90be9b3c9b5c8973eff514e0b1c9@ec2-54-225-203-79.compute-1.amazonaws.com:5432/d2qprvgu3mfhtt")
cur = conn.cursor()
columns = [
    "title",
    "authors",
    "price",
    "link",
    "image",
    "rating",
    "created_ts",
    "created_by"
]
with open('stepik.csv', 'r') as courses:
    courses_dict = csv.DictReader(courses, fieldnames=columns)
    for course in courses_dict:
        print(course)
        cur.execute(
            """
            insert into
             courses(course_name, course_price, course_description, create_time, rating, image)
            values('{title}', {price}, '{link}', '{created_ts}', {rating}, '{image}')
            """.format(
                title=course["title"].strip(r"b\'").replace("'",""),
                price=course["price"],
                link=course["link"],
                created_ts=course["created_ts"],
                rating=course["rating"],
                image=course["image"]
            )
        )

conn.commit()
conn.close()
