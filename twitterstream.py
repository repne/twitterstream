import base64
import httplib
import urllib

def chars(response):
    while not response.isclosed():
        yield response.read(1)

def lines(response):
    buffer = ''
    for char in chars(response):
        if char == '\n':
            yield buffer
            buffer = ''
        else:
            buffer += char

def chunks(response):
    for line in lines(response):
        length = line.strip()
        if not length.isdigit():
            continue
        length = int(length)
        yield response.read(length)
    
def open(host, url, username, password, body):
    headers = {
        'Content-type': 'application/x-www-form-urlencoded',
        'Authorization': 'Basic %s' % base64.b64encode('%s:%s' % (username, password))
    }
    connection = httplib.HTTPConnection(host)
    connection.connect()
    connection.sock.settimeout(90)
    body = urllib.urlencode(body)
    connection.request('POST', url, body, headers=headers)
    return connection.getresponse()

def filter(username, password, track):
    body = dict(track=track, delimited='length')
    return chunks(open('stream.twitter.com', '/1/statuses/filter.json', username, password, body))