#! /usr/bin/env python3
#
# Copyright (c) 2020 Gilles Chehade <gilles@poolp.org>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

import base64
import json
import requests
from opensmtpd import table

def check(token, tableName, service, key):
    res = requests.get('http://localhost:8500/v1/kv/%s/%s/%s' % (tableName, service, key))
    if res.status_code == 404:
        return table.boolean(token, False)
    elif res.status_code == 200:
        return table.boolean(token, True)
    
    return table.failure(token)


def lookup(token, tableName, service, key):
    res = requests.get('http://localhost:8500/v1/kv/%s/%s/%s' % (tableName, service, key))
    if res.status_code == 404:
        return table.result(token)
    elif res.status_code == 200:
        value = json.loads(base64.b64decode(res.json()[0]['Value']).decode())
        return table.result(token, value)
    
    return table.failure(token)

last_fetch_key = None
last_keys = None
def fetch(token, tableName, service):
    global last_fetch_key
    global last_keys

    if last_keys is not None:
        idx = last_keys.index(last_fetch_key)
        if idx != len(last_keys) - 1:
            last_fetch_key = last_keys[idx+1]
            return table.result(token, last_fetch_key)

    last_keys = None
    last_fetch_key = None
    res = requests.get('http://localhost:8500/v1/kv/%s/%s/?keys' % (tableName, service))
    if res.status_code == 404:
        return table.result(token)
    elif res.status_code == 200:
        last_keys = [ '/'.join(key.split('/')[2:]) for key in res.json() ]
        last_fetch_key = last_keys[0]
        return table.result(token, last_fetch_key)
            
    return table.failure(token)

def main():
    table.on_check(check)
    table.on_lookup(lookup)
    table.on_fetch(fetch)
    table.dispatch()

if __name__ == "__main__":
    main()
