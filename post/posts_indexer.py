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

from .es_helpers import make_index_config, doc_from_row
from .util import chunks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('hive2elastic_post')

# disable elastic search's confusing logging
logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)

parser = configargparse.get_arg_parser()

parser.add('--db-url', env_var='DB_URL', required=True, help='hive database connection url')
parser.add('--es-url', env_var='ES_URL', required=True, help='elasticsearch connection url')
parser.add('--es-index', env_var='ES_INDEX', help='elasticsearch index name', default='hive_posts')
parser.add('--es-type', env_var='ES_TYPE', help='elasticsearch type name', default='posts')
parser.add('--es-index-reply', env_var='ES_INDEX_REPLY', help='elasticsearch index name', default='hive_replies')
parser.add('--es-type-reply', env_var='ES_TYPE_REPLY', help='elasticsearch type name', default='replies')
parser.add('--bulk-size', env_var='BULK_SIZE', type=int, help='number of records in a single loop', default=500)
parser.add('--max-workers', type=int, env_var='MAX_WORKERS', help='max workers', default=2)
parser.add('--max-bulk-errors', type=int, env_var='MAX_BULK_ERRORS', help='', default=5)

args = parser.parse_args()

conf = vars(args)

es = elasticsearch.Elasticsearch(conf['es_url'])

bulk_errors = 0


def convert_post(row):
    return doc_from_row(row, conf['es_index'], conf['es_type'])


def convert_reply(row):
    return doc_from_row(row, conf['es_index_reply'], conf['es_type_reply'])


def es_sync(db_engine, rows, post_type):
    global bulk_errors, es, conf

    pool = mp.Pool(processes=conf['max_workers'])
    if post_type == 1:
        index_data = pool.map_async(convert_post, rows).get()
    else:
        index_data = pool.map_async(convert_reply, rows).get()
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
        return

    post_ids = [x.post_id for x in rows]
    chunked_id_list = list(chunks(post_ids, 200))

    for chunk in chunked_id_list:
        sql = "DELETE FROM __h2e_posts WHERE post_id IN :ids"
        db_engine.execute(text(sql), ids=tuple(chunk))


def run():
    global conf, es, bulk_errors

    try:
        db_engine = create_engine(conf['db_url'])
        db_engine.execute("SELECT post_id FROM __h2e_posts LIMIT 1")
    except OperationalError:
        raise Exception("Could not connected: {}".format(conf['db_url']))
    except ProgrammingError:
        raise Exception("__h2e_posts table not exists in database")

    if not es.ping():
        raise Exception("Elasticsearch server not reachable")

    index_name = conf['es_index']
    index_type = conf['es_type']

    try:
        es.indices.get(index_name)
    except elasticsearch.NotFoundError:
        logger.info('Creating new index {}'.format(index_name))
        index_config = make_index_config(index_type)
        es.indices.create(index=index_name, body=index_config)

    reply_index_name = conf['es_index_reply']
    reply_index_type = conf['es_type_reply']

    try:
        es.indices.get(reply_index_name)
    except elasticsearch.NotFoundError:
        logger.info('Creating new index {}'.format(reply_index_name))
        index_config = make_index_config(reply_index_type)
        es.indices.create(index=reply_index_name, body=index_config)

    logger.info('Post starting indexing')

    while True:
        start = time.time()

        sql = '''SELECT post_id, author, permlink, category, depth, children, author_rep,
                 flag_weight, total_votes, up_votes, title, img_url, payout, promoted,
                 created_at, payout_at, updated_at, is_paidout, is_nsfw, is_declined,
                 is_full_power, is_hidden, is_grayed, rshares, sc_hot, sc_trend, sc_hot,
                 body, votes,  json FROM hive_posts_cache
                 WHERE post_id IN (SELECT post_id FROM __h2e_posts ORDER BY post_id ASC LIMIT :limit)
                '''

        posts = db_engine.execute(text(sql), limit=conf['bulk_size']).fetchall()
        db_engine.dispose()

        if len(posts) == 0:
            time.sleep(0.5)
            continue

        replies = []
        post_list = []
        for post in posts:
            if post.depth > 0:
                replies.append(post)
            else:
                post_list.append(post)

        es_sync(db_engine, post_list, 1)
        es_sync(db_engine, replies, 2)

        end = time.time()
        logger.info('{} posts indexed in {}'.format(len(posts), (end - start)))


def main():
    run()


if __name__ == "__main__":
    main()
