from searchable import DB
from random import choice
from collections import defaultdict
from time import time,sleep
import sys

choices=range(10)

class A(object):
  pass

class B(object):
  pass

class C(object):
  pass

classes = [A,B,C]


db = DB()
db.create('a')
db.index('x', 'a')
db.index('store', 'a')

count=10**3

# print "base"
# start = time()
# 
# sz = 64
# bm = bytearray(1)
# 
# lt = (1<<0,1<<1,1<<2,1<<3,1<<4,1<<5,1<<6,1<<7)
# bit = 0
# byte = 0
# for i in xrange(count):
#   if bit == 8:
#     byte+=1
#     bit = 0
#     bm.append(0)  
# 
#   bm[byte] |= lt[bit]
#   bit += 1
# print "baseline {0}".format(time()-start)
# print len(bm)


start = time()
for i in xrange(count):
  db.insert({'primary_key':i, 'x': choice(choices), 'store': choice(classes)}, 'a')
print "finish inserting {0}".format(time()-start)


start = time()
counts = defaultdict(int)
c=0
for r in db.select().frm('a'):
  counts[r['x']] += 1
  c+=1
print "Brute force count  finished {0}".format(time()-start)
print c



print len(counts), counts
start = time()
objs = db.frm('a').where(x=2, store=B).all()
selection = len(objs)
print "bitmask count finished  {0}".format(time()-start)

for obj in objs:
  assert obj['x'] == 2
  assert obj['store'] == B
  
#assert selection == counts[2]
db.update('a').set(store=None).where(x=4).execute()
assert len(db.frm('a').where(store=None).all()) == len(db.frm('a').where(x=4).all())


