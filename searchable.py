import itertools
import operator

basedict = dict

class searchable(object):
  def where(self, clause, *args):
    # Todo: cache query
    assert clause.count('?') == len(args)
    for i, arg in enumerate(args):
      clause = clause.replace('?', '_a_[{0}]'.format(i), 1)

    predicate = compile(clause, "<stdin>", 'eval')
    globals = {'_a_': args}
    return self.__class__(filter(lambda r: eval(predicate, globals, r if isinstance(r,basedict) else r.__dict__), self))
    
  def order_by(self, key):
    return self.__class__(sorted(self, key=operator.attrgetter(key)))
  

class list(list, searchable):
  pass
  
class set(set, searchable):
  pass
      
class dict(dict):
  def where(self, clause, *args):
    return list(self.values()).where(clause, *args)
  