# Скрипт для выдачи списка актеров с количеством их ролей, предыдущие ответы вполне ищутся простыми SQL-запросами

import sqlite3
from pprint import pprint

db = sqlite3.connect("db.sqlite").cursor()

db.execute("SELECT * FROM actors")
actors = db.fetchall()
count = {}

for actor_id, actor_name in actors:
    db.execute("SELECT COUNT(actor_id) AS movie_count FROM movie_actors WHERE actor_id = ?", (actor_id,))
    movie_count = db.fetchone()[0]
    count[actor_name] = movie_count
    
pprint(list(sorted(count.items(), key=lambda a: a[1], reverse=True))[:5])
