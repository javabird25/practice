import sys
import sqlite3
import json
import pprint

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


def main():
    elastic = Elasticsearch()
    create_index(elastic)

    db = sqlite3.connect("db.sqlite")
    db.row_factory = sqlite3.Row
    cursor = db.cursor()
    populate_index(elastic, cursor)


def create_index(elastic):
    """Создает индекс, если он еще не существует."""
    elastic.indices.create(
        "movies",
        {
            "settings": {
                "refresh_interval": "1s",
                "analysis": {
                    "filter": {
                        "english_stop": {
                            "type":       "stop",
                            "stopwords":  "_english_"
                        },
                        "english_stemmer": {
                            "type": "stemmer",
                            "language": "english"
                        },
                        "english_possessive_stemmer": {
                            "type": "stemmer",
                            "language": "possessive_english"
                        },
                        "russian_stop": {
                            "type":       "stop",
                            "stopwords":  "_russian_"
                        },
                        "russian_stemmer": {
                            "type": "stemmer",
                            "language": "russian"
                        }
                    },
                    "analyzer": {
                        "ru_en": {
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "english_stop",
                                "english_stemmer",
                                "english_possessive_stemmer",
                                "russian_stop",
                                "russian_stemmer"
                            ]
                        }
                    }
                }
            },

            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "imdb_rating": {
                        "type": "float"
                    },
                    "genre": {
                        "type": "keyword"
                    },
                    "title": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "description": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "director": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "actors_names": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "writers_names": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "actors": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text",
                                "analyzer": "ru_en"
                            }
                        }
                    },
                    "writers": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text",
                                "analyzer": "ru_en"
                            }
                        }
                    }
                }
            }
        },
        ignore=400  # Игнорировать ошибку при уже существующем индексе
    )


def populate_index(elastic, cursor):
    """Перегружает данные в индекс."""
    execute_sqlite_query(cursor)
    for _, info in streaming_bulk(
        elastic,
        document_generator(cursor),
        max_retries=5,
        yield_ok=False,  # Выдавать только ошибки
        raise_on_error=False,
        raise_on_exception=False
    ):
        print(f"Не удалось загрузить документ: {pprint.pformat(info)}", file=sys.stderr)


def execute_sqlite_query(cursor):
    """Выполняет запрос к sqlite, выдающий таблицу с данными о фильмах."""

    # Чудо-запрос, выдающий таблицу в формате, максимально приближенном к необходимому.
    # Маппинг ElasticSearch отличается от результата запроса только наличием вложенных
    # структур "actors" и "writers", заполнять которые придется с помощью Python.
    cursor.execute("""
    SELECT
        m.id,
        m.imdb_rating,
        m.genre,
        m.title,
        m.plot AS description,
        m.director,

        -- Склейка имен актеров через запятую
        (
            SELECT GROUP_CONCAT(name, ', ')
            FROM (
                -- Результат этого подзапроса - список имен актеров,
                -- снявшихся в текущем обрабатываемом фильме.
                SELECT a.name
                FROM actors a
                WHERE a.id IN (
                    -- Результат этого подзапроса - список id актеров,
                    -- снявшихся в текущем обрабатываемом фильме.
                    SELECT ma.actor_id
                    FROM movie_actors ma
                    WHERE ma.movie_id = m.id
                )
            )
        ) AS actors_names,

        (
            SELECT GROUP_CONCAT(actor_id)
            FROM (
                -- Результат этого подзапроса - список id актеров,
                -- снявшихся в текущем обрабатываемом фильме.
                SELECT ma.actor_id
                FROM movie_actors ma
                WHERE ma.movie_id = m.id
            )
        ) AS actors_ids,

        CASE WHEN LENGTH(TRIM(m.writer)) == 0 THEN m.writers ELSE m.writer END AS writers_ids
    FROM movies m
    """)


def document_generator(cursor):
    """
    Генератор документов, который достает записи из очереди вывода sqlite,
    преобразует их в формат, ожидаемый :func:`elasticsearch.helpers.bulk`
    и возвращает их.
    """
    while row := cursor.fetchone():
        doc = convert_row_to_document(cursor.connection, row)
        yield doc


def convert_row_to_document(db, row):
    """Преобразует один ряд sqlite в документ ElasticSearch."""
    doc = {
        "_index": "movies",
        "_id": row["id"],
    }

    for field in row.keys():
        # Эти поля требуют специальной обработки
        if field in ["actors_ids", "writers_ids"]:
            continue
        # Все остальные можно просто скопировать из ряда
        doc[field] = row[field]

    if doc["imdb_rating"] == "N/A":
        doc["imdb_rating"] = None
    else:
        doc["imdb_rating"] = float(doc["imdb_rating"])
    doc["actors"] = get_actors(db, row)
    writers = doc["writers"] = get_writers(db, row)
    doc["writers_names"] = ", ".join([writer["name"] for writer in writers])

    return doc


def get_person_name(table, db, writer_id):
    """Возвращает имя сценариста или актера по его id."""
    cursor = db.execute(f"SELECT name FROM {table} WHERE id = ?", (writer_id,))
    return cursor.fetchone()["name"]


def get_writer_name(db, writer_id):
    """Возвращает имя сценариста по его id."""
    return get_person_name("writers", db, writer_id)


def get_actor_name(db, writer_id):
    """Возвращает имя актера по его id."""
    return get_person_name("actors", db, writer_id)


def get_actors(db, row):
    """Возвращает dict со структурой, требуемой полем "actors"."""
    actors = []
    for id_ in row["actors_ids"].split(","):
        actors.append({
            "id": int(id_),
            "name": get_actor_name(db, id_),
        })
    return actors


def get_writers(db, row):
    """Возвращает dict со структурой, требуемой полем "writers"."""
    writers = []
    writers_ids = row["writers_ids"]

    # Один сценарист
    if writers_ids[0] != "[":
        writers.append({
            "id": writers_ids,
            "name": get_writer_name(db, writers_ids)
        })
        return writers

    # Несколько сценаристов в JSON-объекте
    writers_ids = list(dict.fromkeys([writer["id"] for writer in json.loads(writers_ids)]))
    for id_ in writers_ids:
        writers.append({
            "id": id_,
            "name": get_writer_name(db, id_)
        })
    return writers


main()
