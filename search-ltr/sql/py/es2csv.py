import os
import time
import json
import codecs
import elasticsearch
import progressbar
from backports import csv
from functools import wraps


FLUSH_BUFFER = 1000  # Chunk of docs to flush in temp file
CONNECTION_TIMEOUT = 120
TIMES_TO_TRY = 3
RETRY_DELAY = 60

META_FIELDS = [u'iid',u'attrValues', u'preCategoryList', u'uid', u'price',u'city', u'updatetm', u'sellcnt', u'contentType', u'evlcnt', u'collectcnt', u'replyCnt', u'add_cart_count', u'title_smart', u'categoryId']
NORMAL_FIELDS = [u'iid',u'uid', u'price', u'updatetm', u'sellcnt', u'contentType', u'evlcnt', u'collectcnt', u'replyCnt', u'add_cart_count']

# Retry decorator for functions with exceptions
def retry(ExceptionToCheck, tries=TIMES_TO_TRY, delay=RETRY_DELAY):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries = tries
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    print(e)
                    print('Retrying in {} seconds ...'.format(delay))
                    time.sleep(delay)
                    mtries -= 1
                else:
                    print('Done.')
            try:
                return f(*args, **kwargs)
            except ExceptionToCheck as e:
                print('Fatal Error: {}'.format(e))
                exit(1)

        return f_retry

    return deco_retry


class Es2csv(object):

    def __init__(self, url, index, type, output_file, debug, max_size):
        #self.opts = opts
        self.url = url
        self.index = index
        self.type = type
        self.output_file = output_file
        self.debug = debug
        self.max_results = max_size
        self.num_results = 0
        self.scroll_ids = []
        self.scroll_time = '30m'
        self.tmp_file = '{}.tmp'.format(output_file)
        self.csv_headers = list(META_FIELDS)


    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def create_connection(self):
        #es = elasticsearch.Elasticsearch(self.opts.url, timeout=CONNECTION_TIMEOUT, http_auth=self.opts.auth,
                                         #verify_certs=self.opts.verify_certs, ca_certs=self.opts.ca_certs,
                                         #client_cert=self.opts.client_cert, client_key=self.opts.client_key)
        es = elasticsearch.Elasticsearch(self.url)
        es.cluster.health()
        self.es_conn = es

    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def check_indexes(self):
        if not self.es_conn.indices.exists(self.index):
            print('no index found')
            exit(1)
        #if '_all' in indexes:
            #indexes = ['_all']
        #else:
            #indexes = [index for index in indexes if self.es_conn.indices.exists(index)]
            #if not indexes:
                #print('Any of index(es) {} does not exist in {}.'.format(', '.join(self.opts.index_prefixes), self.opts.url))
                #exit(1)
        #self.opts.index_prefixes = indexes


    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def search_query(self):
        @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
        def next_scroll(scroll_id):
            return self.es_conn.scroll(scroll=self.scroll_time, scroll_id=scroll_id)

        '''
        search_args = dict(
            index=','.join(self.opts.index_prefixes),
            sort=','.join(self.opts.sort),
            scroll=self.scroll_time,
            size=self.opts.scroll_size,
            terminate_after=self.opts.max_results
        )

        if self.opts.doc_types:
            search_args['doc_type'] = self.opts.doc_types

        if self.opts.query.startswith('@'):
            query_file = self.opts.query[1:]
            if os.path.exists(query_file):
                with codecs.open(query_file, mode='r', encoding='utf-8') as f:
                    self.opts.query = f.read()
            else:
                print('No such file: {}.'.format(query_file))
                exit(1)

        if self.opts.raw_query:
            try:
                query = json.loads(self.opts.query)
            except ValueError as e:
                print('Invalid JSON syntax in query. {}'.format(e))
                exit(1)
            search_args['body'] = query
        else:
            query = self.opts.query if not self.opts.tags else '{} AND tags: ({})'.format(
                self.opts.query, ' AND '.join(self.opts.tags))
            search_args['q'] = query

        if '_all' not in self.opts.fields:
            search_args['_source_include'] = ','.join(self.opts.fields)
            self.csv_headers.extend([unicode(field, "utf-8") for field in self.opts.fields if '*' not in field])

        if self.opts.debug_mode:
            print('Using these indices: {}.'.format(', '.join(self.debug)))
            print('Query[{0[0]}]: {0[1]}.'.format(
                ('Query DSL', json.dumps(query, ensure_ascii=False).encode('utf8')) if self.opts.raw_query else ('Lucene', query))
            )
            print('Output field(s): {}.'.format(', '.join(self.opts.fields)))
            print('Sorting by: {}.'.format(', '.join(self.opts.sort)))
        '''
        body = {'query': {'match_all': {}}}
        res = self.es_conn.search(self.index, self.type, body, scroll=self.scroll_time)
        self.num_results = res['hits']['total']

        print('Found {} results.'.format(self.num_results))
        if self.debug:
            print(json.dumps(res, ensure_ascii=False).encode('utf8'))

        if self.num_results > 0:
            codecs.open(self.output_file, mode='w', encoding='utf-8').close()
            codecs.open(self.tmp_file, mode='w', encoding='utf-8').close()

            hit_list = []
            total_lines = 0

            widgets = ['Run query ',
                       progressbar.Bar(left='[', marker='#', right=']'),
                       progressbar.FormatLabel(' [%(value)i/%(max)i] ['),
                       progressbar.Percentage(),
                       progressbar.FormatLabel('] [%(elapsed)s] ['),
                       progressbar.ETA(), '] [',
                       progressbar.FileTransferSpeed(unit='docs'), ']'
                       ]
            bar = progressbar.ProgressBar(widgets=widgets, maxval=self.num_results).start()

            while total_lines != self.num_results:
                if res['_scroll_id'] not in self.scroll_ids:
                    self.scroll_ids.append(res['_scroll_id'])

                if not res['hits']['hits']:
                    print('Scroll[{}] expired(multiple reads?). Saving loaded data.'.format(res['_scroll_id']))
                    break
                for hit in res['hits']['hits']:
                    total_lines += 1
                    bar.update(total_lines)
                    hit_list.append(hit)
                    if len(hit_list) == FLUSH_BUFFER:
                        self.flush_to_file(hit_list)
                        hit_list = []
                    if self.max_results:
                        if total_lines == self.max_results:
                            self.flush_to_file(hit_list)
                            print('Hit max result limit: {} records'.format(self.max_results))
                            return
                res = next_scroll(res['_scroll_id'])
            self.flush_to_file(hit_list)
            bar.finish()

    def flush_to_file(self, hit_list):
        def to_keyvalue_pairs(source, ancestors=[], header_delimeter='.'):
            def is_list(arg):
                return type(arg) is list

            def is_dict(arg):
                return type(arg) is dict

            if is_dict(source):
                for key in source.keys():
                    to_keyvalue_pairs(source[key], ancestors + [key])

            elif is_list(source):
                if self.opts.kibana_nested:
                    [to_keyvalue_pairs(item, ancestors) for item in source]
                else:
                    [to_keyvalue_pairs(item, ancestors + [str(index)]) for index, item in enumerate(source)]
            else:
                header = header_delimeter.join(ancestors)
                if header not in self.csv_headers:
                    self.csv_headers.append(header)
                try:
                    out[header] = '{}{}{}'.format(out[header], self.opts.delimiter, source)
                except:
                    out[header] = source

        with codecs.open(self.tmp_file, mode='a', encoding='utf-8') as tmp_file:
            for hit in hit_list:
                source= hit['_source']

                out = {field: source.get(field, "") for field in META_FIELDS}
                if '_source' in hit and len(hit['_source']) > 0:
                    #to_keyvalue_pairs(hit['_source'])
                    tmp_file.write('{}\n'.format(json.dumps(out)))
        tmp_file.close()

    def write_to_csv(self):
        if self.num_results > 0:
            self.num_results = sum(1 for line in codecs.open(self.tmp_file, mode='r', encoding='utf-8'))
            if self.num_results > 0:
                #output_file = codecs.open(self.output_file, mode='a', encoding='utf-8')
                fout = open('./esitem_result', 'w')
                #csv_writer = csv.DictWriter(output_file, fieldnames=self.csv_headers)
                #csv_writer.writeheader()
                timer = 0
                widgets = ['Write to csv ',
                           progressbar.Bar(left='[', marker='#', right=']'),
                           progressbar.FormatLabel(' [%(value)i/%(max)i] ['),
                           progressbar.Percentage(),
                           progressbar.FormatLabel('] [%(elapsed)s] ['),
                           progressbar.ETA(), '] [',
                           progressbar.FileTransferSpeed(unit='lines'), ']'
                           ]
                bar = progressbar.ProgressBar(widgets=widgets, maxval=self.num_results).start()

                for line in codecs.open(self.tmp_file, mode='r', encoding='utf-8'):
                    timer += 1
                    bar.update(timer)
                    json_line = json.loads(line)
                    line_formated = list()
                    field_1 = json_line["attrValues"]
                    if field_1:
                        items = list()
                        for item in field_1:
                            length = item.split(";")
                            if length >= 2:
                                for v in length:
                                   vv = v.split(",")
                                   for vvv in vv:
                                       items.append(vvv)
                            items.append(item)
                        values = ','.join(items)
                        line_formated.append(values)
                    else:
                         line_formated.append("")

                    field_2 = json_line["preCategoryList"]
                    if field_2:
                        items = list()
                        for item in field_2:
                            items.append(item)
                        values = ','.join(items)
                        line_formated.append(values)
                    else:
                        line_formated.append("")

                    for field in NORMAL_FIELDS:
                        #print(json_line[field])
                        if json_line[field] == 582446:
                            print('here')
                        line_formated.append(str(json_line[field]))
                        #print(line_formated)

                    line_formated.append(json_line['title_smart'])
                    line_formated.append(str(json_line['categoryId']))
                    formated_line = ';'.join(line_formated)
                    fout.write(formated_line)
                    fout.write('\n')
                    #csv_writer.writerow(formated_line)
                fout.close()
                bar.finish()
            else:
                print('There is no docs with selected field(s): {}.'.format(','.join(self.opts.fields)))
            os.remove(self.tmp_file)

    def clean_scroll_ids(self):
        try:
            self.es_conn.clear_scroll(body=','.join(self.scroll_ids))
        except:
            pass



if __name__ == '__main__':

    url = 'http://172.16.9.39:9200'
    index = 'index'
    type = 'item'
    output_file = 'esitem_result'
    debug = True
    max_size = 1000000
    es = Es2csv(url, index, type, output_file, debug, max_size)
    es.create_connection()
    es.check_indexes()
    es.search_query()
    es.write_to_csv()
    es.clean_scroll_ids()

