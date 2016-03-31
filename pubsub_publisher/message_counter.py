from google.appengine.ext import ndb

class MessageCounter(ndb.Model):
    cursor = ndb.IntegerProperty(indexed=True)

    @staticmethod
    def getCounter():
    	all = MessageCounter.query().fetch()
    	if len (all) > 0:
    		record = all[0]
    		return record.cursor

    	return -1



    @staticmethod
    def writeNewCounter(newPos):
    	ds = MessageCounter(cursor = newPos, id='singleton')
        ds.put()
