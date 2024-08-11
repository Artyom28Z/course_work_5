import psycopg2


class DbManager:
    def __init__(self, db_name, params):
        self.__db_name = db_name
        self.__params = params

    def get_companies_end_vacancies_count(self):
        with psycopg2.connect(db_name=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT companies.name, COUNT(*) AS vacancies_count
                FROM companies
                JOIN vacancies USING (company_id)
                GROUP BY companies.name
                """)
                result = cursor.fetchall()
        conn.close()
        return result

    def get_all_vacancies(self):
        with psycopg2.connect(db_name=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT companies.name, vacancies.name, salary_from, salary_to, vacancies.url 
                FROM vacancies
                JOIN companies USING (company_id) 
                """)
                result = cursor.fetchall()
        conn.close()
        return result

    def get_avg_salary(self):
        with psycopg2.connect(db_name=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT CAST(AVG(salary_from) as INT) 
                FROM vacancies
                """)
                result = cursor.fetchall()
        conn.close()
        return result

    def get_vacancies_with_higher_salary(self, keyword: str):
        with psycopg2.connect(db_name=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                SELECT * FROM vacancies
                WHERE name LIKE '%{keyword}%' OR requirements LIKE '%{keyword}%'
                ORDER BY salary_from DESC
                """)
                result = cursor.fetchall()
        conn.close()
        return result
