from urllib2 import urlopen

symbols = set()
for line in open('../data/ticker.tsv').readlines():
    symbols.add(line.split('\t')[4])

wiki_pages = set()
for line in open('../data/wikipedia-20150623-120000.tsv'):
    wiki_pages.add(line.split('\t')[4])

overlap = []

url = 'http://finance.yahoo.com/d/quotes.csv?s=' + "+".join(symbols) + '&f=sn'
symbol_names = {}
for line in urlopen(url).readlines():
    tokens = line[:-1].split(',', 1)
    symbol = tokens[0][1:-1]
    name = tokens[1][1:-1].replace(' ', '_')
    if name == '/':
        continue
    if name in wiki_pages:
        overlap.append(name)
        symbol_names[symbol] = name
        continue
    if name.endswith('_Common_Stock'):
        name = name[:-len('_Common_Stock')]
        if name in wiki_pages:
            overlap.append(name)
            symbol_names[symbol] = name
            continue
    if name.endswith(',_Inc.'):
        name = name[:-len(',_Inc.')]
        if name in wiki_pages:
            overlap.append(name)
            symbol_names[symbol] = name
            continue

#print("# symbols: " + str(len(symbol_names)))
#print("# wiki pages: " + str(len(wiki_pages)))
#print("overlap: " + str(len(overlap)))

for symbol, name in symbol_names.iteritems():
  print(symbol + '\t' + name)

