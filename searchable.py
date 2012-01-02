import itertools
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
  