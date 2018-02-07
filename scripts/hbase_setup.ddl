create 'hbasePerfTest', 'payload'

put 'hbasePerfTest', 'aa', 'payload:c1', '1234'

t = get_table 'hbasePerfTest'

t.put 'Aa', 'payload:c1', '4567'
t.scan