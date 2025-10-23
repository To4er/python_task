from typing import Any

from typing import Dict, List, Any, Optional
import psycopg2
import json
from config import host, database, user, password

class DatabaseManager:
    def __init__(self, host: str, database: str, user: str, password: str):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self._connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self):
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=password)
            print("Connection established")
        except (Exception, psycopg2.Error) as error:
            print(error)

    def disconnect(self):
        if self._connection is not None:
            self._connection.close()
            print("Database connection closed")

    def execute(self, query: str, vars: Optional[tuple] = None):
        with self._connection.cursor() as cursor:
            cursor.execute(query, vars)
            self._connection.commit()

    def fetch_all(self, query: str) -> List[Dict[str, Any]]:
        with self._connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = []

            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            return results


class JSONHandler:

    @staticmethod
    def get_data(json_file):
        with open(json_file) as json_file:
            data = json.load(json_file)
            return data

    @staticmethod
    def save(data, file_name):
        with open(file_name, "w") as json_file:
            json.dump(data, json_file, indent=4)

class DataManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def list_of_rooms_and_students_on_them(self):
        data = self.db_manager.fetch_all("""
            SELECT r.id, r.name, COUNT(*)
            FROM students s
            JOIN room r ON s.room = r.id
            GROUP BY r.id
            ORDER BY r.id ASC""")
        JSONHandler.save(data, "rooms_and_students_on_them.json")

    def list_of_the_youngest_room(self):
        data = self.db_manager.fetch_all("""
            SELECT r.name, (AVG('2025-10-21'-s.birthday)/365.0)::float AS avarage_age
            FROM students s
            JOIN room r ON r.id = s.room
            GROUP BY r.name
            ORDER BY avarage_age ASC
            LIMIT 5 
        """)
        JSONHandler.save(data, "youngest_rooms.json")

    def list_of_rooms_with_large_age_difference(self):
        data = self.db_manager.fetch_all("""
            SELECT 
                r.name,
                TO_CHAR(MAX(s.birthday), 'YYYY-MM-DD') AS youngest,
                TO_CHAR(MIN(s.birthday), 'YYYY-MM-DD') AS oldest,
                ((MAX(s.birthday) - MIN(s.birthday))/365) AS age_diffrence
            FROM students s
            JOIN room r ON r.id = s.room
            GROUP BY r.name
            ORDER BY age_diffrence DESC
            LIMIT 5
        """)
        JSONHandler.save(data, "rooms_with_large_age_difference.json")

    def list_of_multisex_rooms(self):
        data = self.db_manager.fetch_all("""
            SELECT 
                r.name,
                COUNT(DISTINCT(s.sex)) AS unique_sexes
            FROM students s
            JOIN room r ON r.id = s.room
            GROUP BY r.name
            HAVING COUNT(DISTINCT(s.sex)) = 2
        """)
        JSONHandler.save(data, "multisex_rooms.json")

    def initialize_tables(self):
        tables_sql = ["""
            CREATE TABLE IF NOT EXISTS room (
            id SERIAL PRIMARY KEY,
            name VARCHAR(128))    
        """,
        """
        CREATE TABLE IF NOT EXISTS students (
            birthday DATE, 
            id SERIAL PRIMARY KEY, 
            name VARCHAR(128),
            room int REFERENCES room(id),
            sex VARCHAR(8))
        """]

        for table in tables_sql:
            self.db_manager.execute(table)

    def insert_data(self):

        rooms = JSONHandler.get_data("rooms.json")
        for room in rooms:
            query = """
            INSERT INTO room (id, name)
            VALUES (%s,%s)
            """

            params = (room["id"], room["name"])
            self.db_manager.execute(query, params)

        students = JSONHandler.get_data("students.json")
        for student in students:
            query ="""
                INSERT INTO students (birthday, id, name, room, sex)
                 VALUES (%s, %s, %s, %s, %s)
            """
            params = (student["birthday"], student["id"], student["name"], student["room"], student["sex"])
            self.db_manager.execute(query, params)


if __name__ == "__main__":
    with DatabaseManager(host, database, user, password) as db_manager:
        datamanager = DataManager(db_manager)
        datamanager.initialize_tables()
        datamanager.insert_data()
        datamanager.list_of_rooms_and_students_on_them()
        datamanager.list_of_rooms_with_large_age_difference()
        datamanager.list_of_multisex_rooms()
        datamanager.list_of_the_youngest_room()
