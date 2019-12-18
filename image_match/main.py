from flask import Flask, request
from elasticsearch import Elasticsearch
from image_match.elasticsearch_driver import SignatureES
import json

app = Flask(__name__)
es = Elasticsearch(['127.0.0.1'])
ses = SignatureES(es)

@app.route('/init')
def init():
    rec = { 'filename': 'foo' }
    ses.insert_single_record(rec)
    ses.delete_duplicates('foo')
    return 'OK'

@app.route('/search/filename')
def searchByFilename():
    return json.dumps(ses.search_record(request.args.get('filename')))

@app.route('/search/url')
def searchByUrl():
    q = request.args.get('query')
    query = None
    if q:
        query = json.loads(q)
    c = request.args.get('cutoff')
    cutoff = 0.45
    if c:
        cutoff = float(c)
    return json.dumps(ses.search_image(request.args.get('url'), query=query, distance_cutoff=cutoff))

@app.route('/search/data', methods=['POST'])
def searchByData():
    q = request.args.get('query')
    query = None
    if q:
        query = json.loads(q)
    c = request.args.get('cutoff')
    cutoff = 0.45
    if c:
        cutoff = float(c)
    return json.dumps(ses.search_image(request.stream.read(), bytestream=True, query=query, distance_cutoff=cutoff))

@app.route('/index/url')
def indexByUrl():
    filename = request.args.get('filename')
    ses.delete_duplicates(filename)
    metadata = None
    md = request.args.get('metadata')
    if md:
        metadata = json.loads(md)
    ses.add_image(filename, request.args.get('url'), metadata=metadata)
    return 'OK'

@app.route('/index/data', methods=['POST'])
def indexByData():
    filename = request.args.get('filename')
    ses.delete_duplicates(filename)
    metadata = None
    md = request.args.get('metadata')
    if md:
        metadata = json.loads(md)
    ses.add_image(filename, request.stream.read(), bytestream=True, metadata=metadata)
    return 'OK'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)