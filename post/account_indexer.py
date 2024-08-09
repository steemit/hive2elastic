import logging
import multiprocessing as mp
import sys
import time

import certifi
import configargparse
import elasticsearch
from elasticsearch import helpers
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

from .es_helpers import make_account_index_config, doc_from_row_account
from .util import chunks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('hive2elastic_account')

# disable elastic search's confusing logging
logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)

parser = configargparse.get_arg_parser()

parser.add('--db-url', env_var='DB_URL', required=True, help='hive database connection url')
parser.add('--es-url', env_var='ES_URL', required=True, help='elasticsearch connection url')
parser.add('--es-index', env_var='ES_INDEX_ACCOUNT', help='elasticsearch index name', default='hive_accounts')
parser.add('--es-type', env_var='ES_TYPE_ACCOUNT', help='elasticsearch type name', default='accounts')
parser.add('--bulk-size', env_var='BULK_SIZE', type=int, help='number of records in a single loop', default=500)
parser.add('--max-workers', type=int, env_var='MAX_WORKERS', help='max workers', default=2)
parser.add('--max-bulk-errors', type=int, env_var='MAX_BULK_ERRORS', help='', default=5)

args = parser.parse_args()

conf = vars(args)

es = None
bulk_errors = 0


def convert_account(row):
    return doc_from_row_account(row, conf['es_index'], conf['es_type'])


def run():
    global conf, es, bulk_errors

    try:
        db_engine = create_engine(conf['db_url'])
        db_engine.execute("SELECT account_id FROM __h2e_accounts LIMIT 1")
    except OperationalError:
        raise Exception("Could not connected: {}".format(conf['db_url']))
    except ProgrammingError:
        raise Exception("__h2e_accounts table not exists in database")

    es = elasticsearch.Elasticsearch(conf['es_url'])

    if not es.ping():
        raise Exception("Elasticsearch server not reachable")

    index_name = conf['es_index']
    index_type = conf['es_type']

    try:
        es.indices.get(index_name)
    except elasticsearch.NotFoundError:
        logger.info('Creating new index {}'.format(index_name))
        index_config = make_account_index_config(index_type)
        es.indices.create(index=index_name, body=index_config)

    logger.info('Account starting indexing')

    while True:
        start = time.time()

        sql = '''SELECT id as account_id, name, display_name, profile_image, 
                 followers, following, post_count, reputation, rank, created_at
                 FROM hive_accounts
                 WHERE id IN (SELECT account_id FROM __h2e_accounts ORDER BY account_id ASC LIMIT :limit)
                '''

        accounts = db_engine.execute(text(sql), limit=conf['bulk_size']).fetchall()
        db_engine.dispose()

        if len(accounts) == 0:
            time.sleep(0.5)
            continue

        pool = mp.Pool(processes=conf['max_workers'])
        index_data = pool.map_async(convert_account, accounts).get()
        pool.close()
        pool.join()

        try:
            helpers.bulk(es, index_data)
            bulk_errors = 0
        except helpers.BulkIndexError as ex:
            bulk_errors += 1
            logger.error("BulkIndexError occurred. {}".format(ex))

            if bulk_errors >= conf['max_bulk_errors']:
                sys.exit(1)

            time.sleep(1)
            continue

        account_ids = [x.account_id for x in accounts]
        chunked_id_list = list(chunks(account_ids, 200))

        for chunk in chunked_id_list:
            sql = "DELETE FROM __h2e_accounts WHERE account_id IN :ids"
            db_engine.execute(text(sql), ids=tuple(chunk))

        end = time.time()
        logger.info('{} accounts indexed in {}'.format(len(accounts), (end - start)))


def main():
    run()


if __name__ == "__main__":
    main()
