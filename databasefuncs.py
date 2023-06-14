import sqlite3
import pandas as pd


class MySqliteDatabase:
    def __init__(self):
        self.conn = sqlite3.connect('ESportsEarnings.db')
        cursor = self.conn.cursor()
        cursor.execute('PRAGMA foreign_keys=ON')
        cursor.close()

    def getTableNames(self) -> dict:
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
        cur = self.conn.cursor()
        cur.execute(f'SELECT * FROM {table_name}')
        columns = list(map(lambda x: x[0], cur.description))
        cur.execute(f'SELECT COUNT(*) FROM {table_name}')
        count = cur.fetchall()
        cur.close()
        return {'Column Names': columns, 'Column Count': len(columns), 'Total Rows': count[0][0]}
    
    def getAll(self, table_name, column=None, order=None) -> list:
        """
        If column is not specified, order has no usage.\n
        order is either ASC or DESC
        """
        cur = self.conn.cursor()

        columns = self.getColumnNames(table_name)['Column Names']

        if column in columns and order != None:
            if order == 'ASC':
                cur.execute(f'SELECT * FROM {table_name} ORDER BY {column} ASC')
            elif order == 'DESC':
                cur.execute(f'SELECT * FROM {table_name} ORDER BY {column} DESC')
        else:
            cur.execute(f'SELECT * FROM {table_name}')
        table = cur.fetchall()
        cur.close()
        table_content = []
        for list in table:
            table_content.append(dict(zip(columns, list)))
        return table_content
    

    def search(self, table_name:str, columns:list) -> list:
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
        
