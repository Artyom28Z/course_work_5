import psycopg2


class DbCreator:
    def __init__(self, db_name, params):
        self.__db_name = db_name
        self.__params = params

    def create_database(self):
        conn = psycopg2.connect(db_name="postgres", **self.__params)
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(f'DROP DATABASE IF EXISTS {self.__db_name}')
            cursor.execute(f'CREATE DATABASE {self.__db_name}')
        conn.close()

    def create_tables(self):
        with psycopg2.connect(db_name=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE companies (
                campany_ia INT PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                url VARCHAR(100)
                )
                """)

            with conn.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE vacancies (
                vacancy_id INT PRIMARY KEY,
                company_id INT REFERENCES companies(company_id),
                name VARCHAR(200) NOT NULL,
                city VARCHAR(50),
                url VARCHAR(100),
                salary_from INT,
                salary_to INT,
                requirements TEXT
                )
                """)
        conn.close()

    def save_data_to_db(self, vacancies: list[HhVacancy], companies: [Company]):
        with psycopg2.connect(db_name=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cursor:
                for company in companies:
                    cursor.execute("""
                    INSERT INTO companies (company_id, name, url)
                    VALUES (%s, %s, %s)
                    """,
                (company.company_id, company.company_name, company.url)
                    )
                for vacancy in vacancies:
                    cursor.execute("""
                    INSERT INTO vacancies (vacancy_id, company_id, name, city, url, 
                    salary_from, salary_to, requirements)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (vacancy.vacancy_id, vacancy.company_id, vacancy.name, vacancy.city, vacancy.url,
                    vacancy.salary_from, vacancy.salary_to, vacancy.requirements)
                    )
        conn.close()
