import slob

PLAIN_TEXT = 'text/plain; charset=utf-8'

# test.slob uses the default compression (lzma2); the others hold the same
# content with no compression and with bz2, which slobj doesn't support.
for name, compression in (('test.slob', 'lzma2'),
                          ('test-uncompressed.slob', ''),
                          ('test-bz2.slob', 'bz2')):
    with slob.create(name, compression=compression) as w:
        w.add('Hello, Earth!'.encode('utf-8'),
              'earth', 'terra', content_type=PLAIN_TEXT)
        w.add_alias('земля', 'earth')
        w.add('Hello, Mars!'.encode('utf-8'), 'mars',
              content_type=PLAIN_TEXT)
        w.tag("sometag", "xyz")
        w.tag("some.other.tag", "abc")

# Two dictionaries with several articles under one key, for the order of
# equal keys across dictionaries.
for name, articles in (('homographs-1.slob', ('river', 'money', 'plane')),
                       ('homographs-2.slob', ('river', 'money'))):
    with slob.create(name) as w:
        for meaning in articles:
            w.add(f'{name}: {meaning}'.encode('utf-8'), 'bank',
                  content_type=PLAIN_TEXT)
