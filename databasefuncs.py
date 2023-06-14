import sqlite3
import pandas as pd
from hashlib import sha256 as hasher
import base64

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

    def register(self, name:str, surname:str, email:str, password:str, profilepic:bytes, 
                 nationality=None, birthDate=None, gender=None, description=None) -> bool:
        """
        Return booelan value corresponding to registration of user with given parameters.
        """
        cur = self.conn.cursor()
        userExists = cur.execute("SELECT count(email) FROM LoginTable WHERE email=?", (email, )).fetchone()[0] != 0
        if userExists:
            cur.close()
            return False
        else:
            cur.execute("""INSERT INTO LoginTable (name, surname, birthDate, gender, nationality, 
                                email, password, isAdmin, profilePic, userDesc) 
                                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                                (name, surname, birthDate, gender, nationality, email, 
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
        userExists = cur.execute("SELECT count(email) FROM LoginTable WHERE email=?", (email, )).fetchone()[0] != 0
        if not userExists:
            return 0 
        
        hash_pw = create_hash(password)
        
        user_pw = cur.execute("SELECT password FROM LoginTable WHERE email=?", (email, )).fetchone()[0]
        if hash_pw != user_pw:
            return 2
        cur.close()
        return 1

    def delete(self, email:str, password:str) -> int:
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
    
    def post(self, email:str, password:str, arguments:dict) -> int:
        
        pass


    """def kwargsexmple(self, table_name, **kwargs):
        keys, values = [], []
        for key, value in kwargs.items():
            keys.append(key)
            values.append(values)

        columns = self.getColumnNames(table_name)
        for key in keys:
            if key not in columns:
                index = keys.index(key)
                keys.pop(index)
                values.pop(index)"""
        
