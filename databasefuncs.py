import sqlite3
import pandas as pd
from hashlib import sha256 as hasher
from base64 import b64decode, b64encode
from typing import Any


def create_hash(password):
    pw_bytestring = password.encode()
    return hasher(pw_bytestring).hexdigest()


class MySqliteDatabase:
    """
    SQLite3 EsportsEarnings database functions.
    """
    def __init__(self):
        self.conn = sqlite3.connect('ESportsEarnings.db')
        cursor = self.conn.cursor()
        cursor.execute('PRAGMA foreign_keys=ON')
        cursor.close()

    def getTableNames(self) -> dict:
        """
        Returns table names of EsportsEarnings database.
        """
        cur = self.conn.cursor()
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table'")
        tables = cur.fetchall()
        cur.close()
        table_names = []
        for table_name in tables:
            table_names.append(table_name[0])
        if table_names[0] == 'sqlite_sequence':
            table_names.pop(0)
        return {'Table Names': table_names, 'Table Count': len(table_names)}

    def getColumnNames(self, table_name:str) -> dict:
        """
        :param table_name: Table name in database.

        Return column names of given table name in EsportsEarnings database.
        """
        cur = self.conn.cursor()
        cur.execute(f'SELECT * FROM {table_name}')
        columns = list(map(lambda x: x[0], cur.description))
        cur.execute(f'SELECT COUNT(*) FROM {table_name}')
        count = cur.fetchall()
        cur.close()
        return {'Column Names': columns, 'Column Count': len(columns), 'Total Rows': count[0][0]}
    
    def getAll(self, table_name) -> list:
        """
        :param table_name: Table name in database.

        Return all table contents to given table name in EsportsEarnings database.
        """
        cur = self.conn.cursor()

        columns = self.getColumnNames(table_name)['Column Names']

        cur.execute(f'SELECT * FROM {table_name}')
        table = cur.fetchall()
        cur.close()
        table_content = []
        for list in table:
            table_content.append(dict(zip(columns, list)))
        return table_content
    
    def search(self, table_name:str, columns:list) -> list:
        """
        :param table_name: Table name in database.
        :param columns: Column names given table name.

        Return given columns contents to given table name in EsportsEarnings database.
        """
        if columns:
            table_columns = self.getColumnNames(table_name)['Column Names']
            for column in columns:
                if column not in table_columns:
                    index = columns.index(column)
                    columns.pop(index)
            search_columns = ','.join(columns)
            print(search_columns) 
            cur = self.conn.cursor()
            cur.execute(f'SELECT {search_columns} FROM {table_name}')
            table = cur.fetchall()
            cur.close()
        else:
            cur = self.conn.cursor()
            cur.execute(f'SELECT * FROM {table_name}')
            table = cur.fetchall()
            cur.close()
        
        table_content = []
        for list in table:
            table_content.append(dict(zip(columns, list)))
        return table_content

    def register(self, name:str, surname:str, email:str, password:str, profilepic:bytes=None, 
                 nationality=None, birth_date=None, gender=None, description=None) -> bool:
        """
        Return booelan value corresponding to registration of user with given parameters.
        """
        cur = self.conn.cursor()
        user_exists = cur.execute("SELECT count(email) FROM LoginTable WHERE email=?", (email, )).fetchone()[0] != 0
        if user_exists:
            cur.close()
            return False
        else:
            cur.execute("""INSERT INTO LoginTable (name, surname, birthDate, gender, nationality, 
                                email, password, isAdmin, profilePic, userDesc) 
                                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                                (name, surname, birth_date, gender, nationality, email, 
                                 create_hash(password), 0, profilepic, description))
            cur.fetchall()
            self.conn.commit()
            cur.close()
            return True

    def login(self, email:str, password:str) -> int:
        """
        :param email: E-Mail address
        :param password: Password 

        Returns an integer, 0: User is not registered, 1: Login Successful, 2: Incorrect password
        """
        cur = self.conn.cursor()
        user_exists = cur.execute("SELECT count(email) FROM LoginTable WHERE email=?", (email, )).fetchone()[0] != 0
        if not user_exists:
            return 0 
        
        hash_pw = create_hash(password)
        
        user_pw = cur.execute("SELECT password FROM LoginTable WHERE email=?", (email, )).fetchone()[0]
        if hash_pw != user_pw:
            return 2
        cur.close()
        return 1

    def deleteUser(self, email:str, password:str) -> int:
        """
        :param email: E-Mail address
        :param password: Password 

        Returns an integer, 0: User is not found, 1: Deletion Successful, 2: Incorrect password
        """
        cur = self.conn.cursor()
        userExists = cur.execute("SELECT count(email) FROM LoginTable WHERE email=?", (email, )).fetchone()[0] != 0
        if not userExists:
            return 0 
        
        hash_pw = create_hash(password)
        
        user_pw = cur.execute("SELECT password FROM LoginTable WHERE email=?", (email, )).fetchone()[0]
        if hash_pw != user_pw:
            return 2
        cur.execute("DELETE FROM LoginTable WHERE email=?", (email, ))
        self.conn.commit()
        cur.close()
        return 1
    
    def changeUserElements(self,email:str, password:str, new_password:str=None, name:str=None, surname:str=None, 
                           profilepic:bytes=None, nationality=None, birth_date=None, gender=None, 
                           description=None) -> int:
        cur = self.conn.cursor()
        user_exists = cur.execute("SELECT count(email) FROM LoginTable WHERE email=?", (email, )).fetchone()[0] != 0
        if not user_exists:
            return 0

        hash_pw = create_hash(password)
        self.conn.text_factory = bytes
        user = cur.execute("SELECT * FROM LoginTable WHERE email=?", (email, )).fetchone()
        self.conn.text_factory = Any

        user_name = user[1].decode('utf-8') if user[1] != None else None
        user_surname = user[2].decode('utf-8') if user[2] != None else None
        user_birth_date = user[3].decode('utf-8') if user[3] != None else None
        user_gender = user[4].decode('utf-8') if user[4] != None else None
        user_nationality = user[5].decode('utf-8') if user[5] != None else None
        user_password = user[7].decode('utf-8')
        user_profile_pic = b64encode(user[9]).decode('utf-8') if user[9] != None else None
        user_description = user[10].decode('utf-8') if user[10] != None else None
        if hash_pw != user_password:
            return 2

        hash_password = create_hash(new_password) if new_password != None else hash_pw
        print(hash_password)
        print(new_password)
        cur.execute("""UPDATE LoginTable SET name=?, surname=?, birthDate=?, gender=?, nationality=?,
                        password=?, profilePic=?, userDesc=? WHERE email=?""", 
                        (name if name != None else user_name, 
                            surname if surname != None else user_surname,
                            birth_date if birth_date != None else user_birth_date,
                            gender if gender != None else user_gender,
                            nationality if nationality != None else user_nationality,
                            hash_password,
                            b64decode(profilepic) if profilepic != None else b64decode(user_profile_pic),
                            description if description != None else user_description,
                            email, ))
        self.conn.commit()
        cur.close()
        return 1
        
