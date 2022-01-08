import slob

import slob
PLAIN_TEXT = 'text/plain; charset=utf-8'
with slob.create('test.slob') as w:
    w.add('Hello, Earth!'.encode('utf-8'),
          'earth', 'terra', content_type=PLAIN_TEXT)
    w.add_alias('земля', 'earth')
    w.add('Hello, Mars!'.encode('utf-8'), 'mars',
          content_type=PLAIN_TEXT)
    w.tag("sometag", "xyz")
    w.tag("some.other.tag", "abc")
