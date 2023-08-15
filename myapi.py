from flask import Flask
from flask_restful import Api, Resource, reqparse, abort
from databasefuncs import MySqliteDatabase
import werkzeug


app = Flask(__name__)
api = Api(app)

table_content_args = reqparse.RequestParser()
table_content_args.add_argument('columns', type=str)

user_args = reqparse.RequestParser()
user_args.add_argument('name', type=str, required=False, help='Name has to be given (name)')
user_args.add_argument('surname', type=str, required=False, help='Surname has to be given (surname)')
user_args.add_argument('birthDate', type=str, required=False)
user_args.add_argument('gender', type=str, required=False)
user_args.add_argument('nationality', type=str, required=False)
user_args.add_argument('email', type=str, required=True, help='E-Mail address has to be given (email)')
user_args.add_argument('password', type=str, required=True, help='Password has to be given (password)')
user_args.add_argument('profilepic', type=werkzeug.datastructures.FileStorage, location='files', required=False)
user_args.add_argument('userDesc', type=str, required=False)
user_args.add_argument('newpassword', type=str, required=False)

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

class User(Resource):
    def get(self):
        args = user_args.parse_args()
        myDB = MySqliteDatabase()
        result = myDB.login(args['email'], args['password'])
        if result == 0:
            return ({"Result": 'This user is not registered.'}, 404)
        elif result == 1:
            return ({"Result": 'Successful login.'}, 200)
        elif result == 2:
            return ({"Result": 'Wrong password.'}, 401)
    
    def delete(self):
        args = user_args.parse_args()
        myDB = MySqliteDatabase()
        result = myDB.deleteUser(args['email'], args['password'])
        print(result)
        if result == 0:
            return ({"Result": "This account does not exist."}, 404)
        elif result == 1:
            return ({"Result": "Successfully deleted the account."}, 200)
        else:
            return ({"Result": "Wrong password, account has not been deleted."}, 401)

    def post(self):
        args = user_args.parse_args()
        myDB = MySqliteDatabase()
        result = myDB.register(email=args['email'], password=args['password'],
                                name=args['name'] if args['name'] != None else None, 
                                surname=args['surname'] if args['surname'] != None else None, 
                                profilepic=args['profilepic'].read() if args['profilepic'] != None else None,
                                nationality=args['nationality'] if args['nationality'] != None else None,
                                birth_date=args['birthDate'] if args['birthDate'] != None else None,
                                gender=args['gender'] if args['gender'] != None else None,
                                description=args['userDesc'] if args['userDesc'] != None else None)
        
        return ({"Result": f"{args['email']} has been registered."}, 201) if result else ({"Result": "This user already exists."}, 409)

    def put(self):
        args = user_args.parse_args()
        myDB = MySqliteDatabase()
        result = myDB.changeUserElements(email=args['email'], password=args['password'], 
                                            new_password=args['newpassword'] if args['newpassword'] != None else None,
                                            name=args['name'] if args['name'] != None else None, 
                                            surname=args['surname'] if args['surname'] != None else None, 
                                            profilepic=args['profilepic'].read() if args['profilepic'] != None else None,
                                            nationality=args['nationality'] if args['nationality'] != None else None,
                                            birth_date=args['birthDate'] if args['birthDate'] != None else None,
                                            gender=args['gender'] if args['gender'] != None else None,
                                            description=args['userDesc'] if args['userDesc'] != None else None)
        if result == 0:
            return ({"Result": "This account does not exist."}, 404)
        elif result == 1:
            return ({"Result": "Successfully changed the informations."}, 200)
        else:
            return ({"Result": "Wrong password, nothing changed."}, 401)
        

api.add_resource(TableNames, '/tableinfo')
api.add_resource(TableColumnNames, '/tableinfo/<string:table_name>')
api.add_resource(TableContentsAll, '/contents/<string:table_name>')
api.add_resource(User, '/user')

if __name__ == '__main__':
    app.run(debug=True)
