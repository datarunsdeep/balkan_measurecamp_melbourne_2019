import json
import os
import smtplib
import constants as const
from flask import Flask, request
from google.cloud import bigquery


app = Flask(__name__)


@app.route('/', methods=['POST'])
def index():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'
    request_data = request.data
    utf_data = request_data.decode("utf-8")
    data_json = json.loads(utf_data)
    client_id = data_json['message']

    # instantiate a bigquery client connection
    bq_client = bigquery.Client()

    headers = {'Access-Control-Allow-Origin': '*'}

    # create the sql query with the input id
    query = """
    SELECT first_name, company, sales_rep 
    FROM `measurecamp-sydney.salesforce.salesforce` 
    WHERE ga_id = '{}'
    """.format(client_id)
    print(query)

    # run the query on bigquery
    query_job = bq_client.query(query)
    results = query_job.result()

    # put results in two variables
    # note: bigquery query returns RowIterator object; to access
    # the contents we have to loop over the iterator.
    name = ''
    company = ''
    sales_rep = ''
    for row in results:
        print("name is {} , sales rep is {} ".format(row.first_name, row.sales_rep))
        name = row.first_name
        company = row.company
        sales_rep = row.sales_rep

    # concatenate result vars into one string and return it
    # (because returning tuples caused 500 server errors)
    res = name + ',' + sales_rep

    # Set up SMTP connection for email
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(const.email, const.password)

    msg = "Hi {}, your prospect {} from {} has just viewed your website.".format(sales_rep, name, company)
    server.sendmail(const.email, const.email, msg)
    server.quit()
    return res, 200, headers


if __name__ == "__main__":
    app.run()
