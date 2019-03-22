#!/usr/bin/env python2
# encoding: utf-8

# Import the library psycopg2 to connect to DataBase
import psycopg2

DataBaseName = "news"
def execute_db(query):

    """Connect to database for querries"""
    try:
        db = psycopg2.connect('dbname=' + DataBaseName)
    except psycopg2.Error as e:
        print("Unable to connect to the database")
        print(e.pgerror)
        print(e.diag.message_detail)
        sys.exit(1)

    c = db.cursor()
    c.execute(query)
    rows = c.fetchall()
    db.close()
    return rows

def popular_articles():

    # Query to answer the taks aswers
	
    query = """
        select articles.title, count(log.path) as num
        from log join articles
        on log.path = CONCAT('/article/', articles.slug) where 
        log.status = '200 OK' 
        group by articles.title 
        order by num Desc limit 3;
    """
    results = executar_db(query)

	#Printing the query results

    print('The three most popular articles of all time are: \n')

    count = 1

    for i in results:
        number = '(' + str(count) + ') "'
        title = i[0]
        views = '" with ' + str(i[1]) + " views"
        print(number + title + views)
        count += 1


def popular_authors():

    query = """
        select authors.name, count(log.path) as num
        from articles join authors
        on articles.author = authors.id
        join log on log.path like CONCAT('/article/', articles.slug) where
        log.status = '200 OK'
        group by authors.name
        order by num Desc limit 3;
    """
    results = executar_db(query)

    # Printing the results of 'def autores_mais_populares()':
    print('The most Popular Authors are: \n')

    count = 1
    for i in results:
        print('(' + str(count) + ') ' + i[0] + ' with ' + str(i[1]) +
              " views")
        count += 1


def days_with_more_than_1_percent_error():

    query = """
        select to_char(time::date, 'Month DD,YYYY'),
        (count(case when status != '200 OK' then 1 end)*100)::float/
        count(*) as num from log 
        group by time::date 
        order by num desc limit 1;
    """
    results = executar_db(query)

    # Printing the results of 'def dias_com_mais_de_1_por_cento_de_erro()':

    print('On what days more than 1% of requests resulted in errors? \n')

    for i in results:
        date = i[0]
        errors = str(round(i[1], 1)) + "%" + " errors"
        print(date + " -- " + errors)

# All the results of the querries
print('Results:')
print('____________________________________')
popular_articles()
print('____________________________________')
popular_authors()
print('____________________________________')
days_with_more_than_1_percent_error()
