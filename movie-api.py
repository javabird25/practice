from copy import deepcopy

from flask import Flask
from flask_restful import Api, Resource, reqparse, inputs, abort
from werkzeug.exceptions import HTTPException
from elasticsearch import Elasticsearch, NotFoundError

app = Flask(__name__)
app.url_map.strict_slashes = False
api = Api(app)


class MovieList(Resource):
    _PARSER = reqparse.RequestParser(bundle_errors=True)
    _PARSER.add_argument(
        "limit",
        type=inputs.positive,
        default=50
    )
    _PARSER.add_argument(
        "page",
        type=inputs.positive,
        default=1
    )
    _PARSER.add_argument(
        "sort",
        type=str,
        choices=["id", "title", "imdb_rating"],
        default="id"
    )
    _PARSER.add_argument(
        "sort_order",
        type=str,
        choices=["asc", "desc"],
        default="asc"
    )
    _PARSER.add_argument(
        "search",
        type=str
    )

    def get(self):
        try:
            args = self._PARSER.parse_args()
        except HTTPException as e:
            e.code = 422
            raise e
        return self._search(args)

    def _search(self, args):
        if not args["search"]:
            return self._make_all_query(args)
        hits = self._make_main_query(args)
        if hits:
            return hits
        else:
            return self._make_fallback_query(args)

    # Главный режим поиска - по названию.
    _MAIN_QUERY = {
        "query": {
            "multi_match": {
                "fields": [
                    "title"
                ]
            }
        }
    }

    # Запасной режим поиска - по остальным полям.
    _FALLBACK_QUERY = {
        "query": {
            "multi_match": {
                "fields": [
                    "description",
                    "director",
                    "actors_names",
                    "writers_names",
                ]
            }
        }
    }

    # Запрос для выдачи всех фильмов - применяется при пустом search.
    _ALL_QUERY = {
        "query": {
            "match_all": {}
        }
    }

    def _make_query(self, args, query):
        elastic = Elasticsearch()

        response = elastic.search(
            query,
            "movies",
            sort=args["sort"] + ":" + args["sort_order"],
            size=args["limit"],
            from_=args["limit"] * (args["page"] - 1),
            _source=["id", "title", "imdb_rating"]
        )
        hits = response["hits"]["hits"]
        documents = map(lambda hit: hit["_source"], hits)
        return list(documents)

    def _make_main_query(self, args):
        query = deepcopy(self._MAIN_QUERY)
        query["query"]["multi_match"]["query"] = args["search"]

        return self._make_query(args, query)

    def _make_fallback_query(self, args):
        query = deepcopy(self._FALLBACK_QUERY)
        query["query"]["multi_match"]["query"] = args["search"]

        return self._make_query(args, query)

    def _make_all_query(self, args):
        return self._make_query(args, self._ALL_QUERY)


api.add_resource(MovieList, "/api/movies")


class Movie(Resource):
    def get(self, movie_id):
        try:
            result = self._make_elasticsearch_request(movie_id)
        except NotFoundError:
            abort(404, message="The movie with this ID does not exist")
            return
        movie = result["_source"]
        self._listify_genres_and_director(movie)
        return movie

    def _listify_genres_and_director(self, movie):
        movie["genre"] = movie["genre"].split(", ")
        movie["director"] = [movie["director"]]

    def _make_elasticsearch_request(self, movie_id):
        elastic = Elasticsearch()
        return elastic.get("movies", movie_id, _source_excludes=["actors_names", "writers_names"])


api.add_resource(Movie, "/api/movies/<string:movie_id>")
