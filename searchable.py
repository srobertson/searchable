import itertools
import functools
import operator

basedict = dict

class searchable(object):
  _key = "__dict__"
  getter = operator.attrgetter(_key)
  
  @property
  def key(self):
    return self._key
  
  @key.setter  
  def key(self,key):
    self.getter = operator.attrgetter(key)
    self._key = key
    
  def where(self, clause,  *args):
    # Todo: cache query
    assert clause.count('?') == len(args)
    for i, arg in enumerate(args):
      clause = clause.replace('?', '_a_[{0}]'.format(i), 1)

    predicate = compile(clause, "<stdin>", 'eval')
    globals = {'_a_': args}

    getter = self.getter
    
    return self.__class__(filter(lambda r: eval(predicate, globals, r if isinstance(r,basedict) else getter(r)), self))
    
  def order_by(self, key):
    return self.__class__(sorted(self, key=operator.attrgetter(key)))
  

class list(list, searchable):
  pass
  
class set(set, searchable):
  pass
      
class dict(dict):
  key = "__dict__"
  def where(self, clause,  *args):
    l = list(self.values())
    l.key = self.key
    return l.where(clause, *args)
  
  
  
#New 
from collections import defaultdict
from itertools import izip


lt = (1<<0,1<<1,1<<2,1<<3,1<<4,1<<5,1<<6,1<<7)    
class BTable(object):
  def __init__(self):
    self.bit = 0
    self.byte = 0
    self.indexes = defaultdict(lambda:defaultdict(lambda :bytearray()))
    self.records = []
    
  def __iter__(self):
    return iter(self.records)
    
  def create_index(self, attr):
    if attr not in self.indexes:
      index = self.indexes[attr]
      for i, record in enumerate(self.records):
        index[record[attr]] |= lt[i]
    
  def insert(self, obj):
    
    row_id = len(self.records)
    self.records.append(obj)
    byte,bit = self.byte, self.bit#divmod(row_id,8)
    
    mask = lt[bit]
    for attr,index in self.indexes.items():
      bm = index[obj.get(attr)]
      while( (byte+1) > len(bm)):
        bm.append(0)
      
      bm[byte] |= mask
    
    self.bit+=1
    if self.bit == 8:
      self.bit = 0
      self.byte += 1
      
  def where(self, **kw):
    barrays = [self.indexes[a][v] for a,v in kw.items()]
    
    byte = 0
    for masks in izip(*barrays):
      bitmap = reduce(lambda x,y: x&y, masks, 255)
      if bitmap > 0:
        for bit in range(8):
          if bitmap & lt[bit]:
            yield self.records[(byte*8) + bit]
      byte+=1

class Query(object):
  def __init__(self, table):
    self._table = table
    self._limit=None
    self._where = {}
    self._order = ()
    
    
  def ids(self):
    """Return ids that match the current query"""
    indexes = self._table.indexes

    if self._where:
      search_plan = []
      uattrs  = [] # unindexed attrs
      uvals  = [] # unindexed vals
      for attr,val in self._where.items():
        # lookup indexes using get to avoid default dict creating new
        # keys
        index = indexes.get(attr)
        if index is not None:
          search_plan.append((len(index),index, val))
        else:
          uattrs.append(attr)
          uvals.append(val)
      
      # sort indexes based on cardinality, higher cardinality to make set intersection
      # faster
      if search_plan:
        search = sorted(search_plan,  reverse=True)
        ignored, index, val = search_plan[0]
        row_ids = index[val].copy()
        for ignored, index, val in search_plan[1:]:
          row_ids &= index[val]
      else: # ug unindexed search
        row_ids = xrange(len(self._table.records))
      
      if not uattrs: # 100% answered by indexes
        return row_ids
       
      def selector(key, *args):
        obj = args[-1]
        return tuple([key(obj, arg) for arg in args[:-1]])

      getter = functools.partial(selector, self._table.key, *uattrs)#operator.itemgetter(*uattrs)
      uvals = tuple(uvals)
      def func(row_id):
        try:
          return getter(self._table.records[row_id]) == uvals
        except LookupError:
          pass
      return itertools.ifilter(func, row_ids )
  
    else:
      return [i for (i,r) in enumerate(self._table.records) if r is not None]
  
  
  def __iter__(self):
    if self._limit is None:
      limit = len(self._table.records)
    else:
      limit = self._limit
      
    for i,row_id in enumerate(self.ids()):
      if i < limit:
        yield self._table.records[row_id]
    
  def first(self):
    try:
      return iter(self).next()
    except StopIteration:
      return None
    
  def all(self):
    return list(iter(self))
    
  def limit(self, limit):
    self.limit = limit
    return self
    
  def where(self,**kw):
    self._where = kw
    return self
    
  def order_by(self, *order):
    self._order = order
    return self
    
class Update(Query):
  def __init__(self, table):
    super(Update,self).__init__(table)
    self._set = {}
    
  def set(self, **kw):
    self._set = kw
    return self
    
  def execute(self):
    if self._set:
      _getter = self._table._getter
      _setter = self._table._setter
      records = self._table.records
      indexes = self._table.indexes
      for row_id in self.ids():
        obj = records[row_id]
        for attr,val in self._set.items():
          old_val = _getter(obj, attr)
          index = indexes.get(attr)
          if index:
            index[old_val].remove(row_id)
            index[val].add(row_id)
            
          # be interesting to call set_value here if we had KVO compliant
          # dictionaries
          
          _setter(obj, attr, val)

class Delete(Query):
  def execute(self):
    """Removes object and clears out the index.
    To avoid rearraning we simply set the record to none and track
    deleted id in a seperate set.
    """
    
    if self._where:
      indexes = self._table.indexes
      _getter = self._table._getter
      records = self._table.records
      deleted = self._table.deleted
      for row_id in self.ids():
        obj = records[row_id]
        records[row_id]=None
        deleted.add(row_id)
        for attr,index in indexes.items():
          val = _getter(obj, attr)
          index[val].remove(row_id)
    else:
      # no where clause, nuke everything
      self._table.records=[]
      self._table.deleted.clear()
      for index in self._table.indexes.values():
        index.clear()
        
    
            

class STable(object):
  """Provides an interface for working with objecs as if they
  lived in a databe. 
  
  Features include multiple values for fast lookup, ordering
  and bulk operations based on querying.
  
  
  """
  def __init__(self, key=operator.getitem):
    self.indexes = defaultdict(lambda:defaultdict(set))
    self.records=[]
    self.deleted = set()
    
    # key is a function used to retreive the value
    # from a record object
    self.key = key
    
  def __iter__(self):
    return iter(self.records)
  
  def _getter(self, record, attr, default=None):
    """Returns the value using the key function or None if the record does not have such value
    """ 
    # shame that python operator.getitem and ilk don't support deafult values 
    try:
      return self.key(record, attr)
    except LookupError:
      return default
      
  def _setter(self, obj,attr,value):
    """Sets the value in the way most appropriate for the object."""
    if isinstance(obj, dict):
      obj[attr] = value
    else:
      setattr(obj, attr, value)    
    
  def create_index(self, attr):
    key = self.key
    if attr not in self.indexes:
      index = self.indexes[attr]
      for i, record in enumerate(self.records):
        index[attr][self._getter(record,attr)].add(i)
        
  def delete(self):
    return Delete(self)

  def insert(self,record):
    row_id = len(self.records)
    self.records.append(record)

    for attr,index in self.indexes.items():
      index[self._getter(record, attr)].add(row_id)
      
  def update(self):
    return Update(self)
      
  def where(self, **kw):
    return Query(self).where(**kw)
    
  def all(self):
    return self.records
    

class DB(object):
  Table = STable
  def __init__(self):
    self.tables = {}

  def create(self, id):
    self.tables[id] = self.Table()

  def index(self, attr, table):
    self.tables[table].create_index(attr)

  def insert(self, obj, table):
    self.tables[table].insert(obj)
    
  def update(self, table):
    return self.tables[table].update()

  def select(self):
    return self

  def frm(self, table):
    return self.tables[table]
    
