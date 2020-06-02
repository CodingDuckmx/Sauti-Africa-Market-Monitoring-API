import os
import psycopg2

from dotenv import load_dotenv, find_dotenv
from flask import current_app as app
from flask import jsonify, request

load_dotenv()

############################################################################################################

'''Verify the credentials before running deployment. '''

############################################################################################################


@app.route('/verifyconn', methods=['GET'])
def verify_db_conn():
    '''
    Verifies the connection to the db.
    '''
    try:

        connection = psycopg2.connect(user=os.environ.get('eleph_db_user'),
                            password=os.environ.get('eleph_db_password'),
                            host=os.environ.get('eleph_db_host'),
                            port=os.environ.get('eleph_db_port'),
                            database=os.environ.get('eleph_db_name'))

        return 'Connection verified.'

    except:

        return 'Connection failed.'

    finally:

        if (connection):
            connection.close()

@app.errorhandler(404)
def page_not_found(e):
    
    return '<h1>Error 404</h1><p> Sorry, I cannot show anything arround here.</p><img src="/static/404.png">', 404


@app.route('/raw/')
def query_raw_data():

    query_parameters = request.args
    product_name = query_parameters.get('product_name')
    market_name = query_parameters.get('market_name')
    country_code = query_parameters.get('country_code')
    source_name = query_parameters.get('source_name')

    connection = psycopg2.connect(user=os.environ.get('eleph_db_user'),
                        password=os.environ.get('eleph_db_password'),
                        host=os.environ.get('eleph_db_host'),
                        port=os.environ.get('eleph_db_port'),
                        database=os.environ.get('eleph_db_name'))

    cursor = connection.cursor()

    if source_name:

        cursor.execute('''
                    SELECT id
                    FROM sources
                    WHERE source_name = %s
        ''', (source_name,))

        source_id = cursor.fetchall()

        if not source_id:

            return 'That source name is not in the db.'

        else:

            source_id = source_id[0][0]

    query = ''' 
            SELECT *
            FROM raw_table
            WHERE
            '''
    to_filter = []


    if product_name:
        query += ' product_name=%s AND'
        to_filter.append(product_name)
    if market_name and country_code:
        market_id = market_name + ' : ' + country_code
        query += ' market_id=%s AND'
        to_filter.append(market_id)
    if source_name:
        cursor.execute('''
                SELECT id
                FROM sources
                WHERE source_name = %s
        ''', (source_name,))

        source_id = cursor.fetchall()

        if source_id:

            source_id = source_id[0][0]
            query += ' source_id = %s AND'
            to_filter.append(source_id)
    if not (product_name and market_name and country_code):
        return page_not_found(404)

    query = query[:-4] + ';'

    cursor.execute(query, to_filter)

    result = cursor.fetchall()

    if result:

        return jsonify(result)
    
    else:
        
        return page_not_found(404)

    if connection:
        
        connection.close()


@app.route('/bc-retail/')
def query_bc_retail_data():

    query_parameters = request.args
    product_name = query_parameters.get('product_name')
    market_name = query_parameters.get('market_name')
    country_code = query_parameters.get('country_code')
    source_name = query_parameters.get('source_name')

    connection = psycopg2.connect(user=os.environ.get('eleph_db_user'),
                        password=os.environ.get('eleph_db_password'),
                        host=os.environ.get('eleph_db_host'),
                        port=os.environ.get('eleph_db_port'),
                        database=os.environ.get('eleph_db_name'))

    cursor = connection.cursor()

    if source_name:

        cursor.execute('''
                    SELECT id
                    FROM sources
                    WHERE source_name = %s
        ''', (source_name,))

        source_id = cursor.fetchall()

        if not source_id:

            return 'That source name is not in the db.'

        else:

            source_id = source_id[0][0]

    query = ''' 
            SELECT *
            FROM bc_retail_prices
            WHERE
            '''
    to_filter = []


    if product_name:
        query += ' product_name=%s AND'
        to_filter.append(product_name)
    if market_name and country_code:
        market_id = market_name + ' : ' + country_code
        query += ' market_id=%s AND'
        to_filter.append(market_id)
    if source_name:
        cursor.execute('''
                SELECT id
                FROM sources
                WHERE source_name = %s
        ''', (source_name,))

        source_id = cursor.fetchall()

        if source_id:

            source_id = source_id[0][0]
            query += ' source_id = %s AND'
            to_filter.append(source_id)
    if not (product_name and market_name and country_code):
        return page_not_found(404)

    query = query[:-4] + ';'

    cursor.execute(query, to_filter)

    result = cursor.fetchall()

    print(result)

    if result:

        return jsonify(result)
    
    else:
        
        return page_not_found(404)

    if connection:
        
        connection.close()

@app.route('/bc-wholesale/')
def query_bc_wholesale_data():

    query_parameters = request.args
    product_name = query_parameters.get('product_name')
    market_name = query_parameters.get('market_name')
    country_code = query_parameters.get('country_code')
    source_name = query_parameters.get('source_name')

    connection = psycopg2.connect(user=os.environ.get('eleph_db_user'),
                        password=os.environ.get('eleph_db_password'),
                        host=os.environ.get('eleph_db_host'),
                        port=os.environ.get('eleph_db_port'),
                        database=os.environ.get('eleph_db_name'))

    cursor = connection.cursor()

    if source_name:

        cursor.execute('''
                    SELECT id
                    FROM sources
                    WHERE source_name = %s
        ''', (source_name,))

        source_id = cursor.fetchall()

        if not source_id:

            return 'That source name is not in the db.'

        else:

            source_id = source_id[0][0]

    query = ''' 
            SELECT *
            FROM bc_wholesale_prices
            WHERE
            '''
    to_filter = []


    if product_name:
        query += ' product_name=%s AND'
        to_filter.append(product_name)
    if market_name and country_code:
        market_id = market_name + ' : ' + country_code
        query += ' market_id=%s AND'
        to_filter.append(market_id)
    if source_name:
        cursor.execute('''
                SELECT id
                FROM sources
                WHERE source_name = %s
        ''', (source_name,))

        source_id = cursor.fetchall()

        if source_id:

            source_id = source_id[0][0]
            query += ' source_id = %s AND'
            to_filter.append(source_id)
    if not (product_name and market_name and country_code):
        return page_not_found(404)

    query = query[:-4] + ';'

    cursor.execute(query, to_filter)

    result = cursor.fetchall()

    print(result)

    if result:

        return jsonify(result)
    
    else:
        
        return page_not_found(404)

    if connection:
        
        connection.close()