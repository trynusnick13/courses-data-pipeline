import argparse
import csv
import datetime
import io
import logging
import sys

import requests
from bs4 import BeautifulSoup
from google.cloud import storage

from config import URLS

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def get_cli_arguments() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape and parse data from Stepik")
    parser.add_argument("--dag_name", type=str, default="dag_stepik_to_bq")
    parser.add_argument("--bucket_name", type=str, default="courses_ingestion")
    parser.add_argument("--path_in_bucket", type=str, default="stepik")
    parser.add_argument("--destination_blob_name", type=str, default="stepik.csv")
    return parser


def upload_blob(
        bucket_name: str,
        file: io.StringIO,
        destination_blob_name: str,
        path_in_bucket: str
) -> None:
    size_of_file = sys.getsizeof(file)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(f"{path_in_bucket}/{destination_blob_name}")
    blob.upload_from_file(file)
    logger.info("File size {} Bytes uploaded to gs://{}.".format(
        size_of_file,
        bucket_name))


def ingest_from_stepik_to_gcs(
        dag_name: str,
        bucket_name: str,
        path_in_bucket: str,
        destination_blob_name: str
) -> None:
    courses = []
    for url in URLS:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        for course in soup.find_all("li", "course-cards__item"):

            title = course.find("a", class_="course-card__title")
            authors = course.find("a", class_="course-card__author")
            price = course.find("span", class_="format-price")
            image = course.find("img", class_="course-card__cover")
            rating = course.find(
                "span",
                class_="course-card__widget",
                attrs={
                    "data-type": "rating"
                }
            )

            if title is None or authors is None or price is None:
                continue

            link = "https://stepik.org" + title.get("href")
            image = "https://stepik.org" + image.get("src")
            title = title.string.strip()
            authors = authors.string.strip()
            price = price.text if "Free" not in price.text else "0"
            rating = rating.text.strip() if rating is not None else 0

            course_extracted = {
                "title": title.encode("utf-8"),
                "authors": authors.encode("utf-8"),
                "price": float(price.replace("$", "")),
                "link": link,
                "image": image,
                "rating": float(rating),
                "created_ts": datetime.datetime.utcnow(),
                "created_by": dag_name
            }
            logger.info("Scraped course {}.".format(course_extracted))
            courses.append(course_extracted)

    with io.StringIO("w", newline="") as file:
        w = csv.DictWriter(
            file,
            fieldnames=[
                "title",
                "authors",
                "price",
                "link",
                "image",
                "rating",
                "created_ts",
                "created_by",
            ],

        )
        for course in courses:
            w.writerow(course)
        file.seek(0)

        upload_blob(
            bucket_name=bucket_name,
            file=file,
            destination_blob_name=destination_blob_name,
            path_in_bucket=path_in_bucket,
        )
        file.flush()


def main():
    parser = get_cli_arguments().parse_args()
    dag_name = parser.dag_name
    bucket_name = parser.bucket_name
    path_in_bucket = parser.path_in_bucket
    destination_blob_name = parser.destination_blob_name

    ingest_from_stepik_to_gcs(
        dag_name=dag_name,
        bucket_name=bucket_name,
        path_in_bucket=path_in_bucket,
        destination_blob_name=destination_blob_name,
    )


if __name__ == '__main__':
    main()
