Курсовая 5. Работа с базами данных

Для запуска проекта необходимо установить библиотеки необходимых версий. Версии зависимосте указаны в файле requirements.txt


В рамках проекта получаются данные о компаниях и вакансиях с сайта hh.ru, спроектировать таблицы в БД PostgreSQL и загрузить полученные данные в созданные таблицы.


Основные шаги проекта:

- Получаем данные о работодателях и их вакансиях с сайта hh.ru. Для этого используйте публичный API hh.ru и библиотеку 
requests

- Выбраем не менее 10 интересных вам компаний, от которых вы будете получать данные о вакансиях по API.
Спроектировать таблицы в БД PostgreSQL для хранения полученных данных о работодателях и их вакансиях. Для работы с БД используйте библиотеку 
psycopg2

- Реализуем код, который заполняет созданные в БД PostgreSQL таблицы данными о работодателях и их вакансиях.

- Создаём класс DBManager для работы с данными в БД.
  

Создаём класс DBManager, который будет подключаться к БД PostgreSQL и иметь следующие методы:

 
- get_companies_and_vacancies_count() — получает список всех компаний и количество вакансий у каждой компании.
 
- get_all_vacancies() — получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
 
- get_avg_salary() — получает среднюю зарплату по вакансиям.
 
- get_vacancies_with_higher_salary() — получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
 
- get_vacancies_with_keyword() — получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python.

*Класс DBManager будет использовать библиотеку psycopg2 для работы с БД.
