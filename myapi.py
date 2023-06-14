from flask import Flask
from flask_restful import Api, Resource, reqparse, abort
from databasefuncs import MySqliteDatabase



app = Flask(__name__)
api = Api(app)

table_content_args = reqparse.RequestParser()
table_content_args.add_argument('columns', type=str)


class TableNames(Resource):
    def get(self):
        myDB = MySqliteDatabase()
        return myDB.getTableNames(), 200


class TableColumnNames(Resource):
    def get(self, table_name):
        myDB = MySqliteDatabase()
        try:
            return myDB.getColumnNames(table_name), 200
        except:
            return abort(400, description='This table does not exists. For more information look /tableinfo')


class TableContentsAll(Resource):
    def get(self, table_name):
        args = table_content_args.parse_args()
        myDB = MySqliteDatabase()
        try:
            if args['columns']:
                return myDB.search(table_name, args['columns'].replace(' ', '').split(','))
            return myDB.getAll(table_name), 200
        except:
            return abort(400, description='This table does not exists. For more information look /tableinfo')


api.add_resource(TableNames, '/tableinfo')
api.add_resource(TableColumnNames, '/tableinfo/<string:table_name>')
api.add_resource(TableContentsAll, '/contents/<string:table_name>')


if __name__ == '__main__':
    app.run(debug=True)
