from google.appengine.ext import ndb

class PublishedMessages(ndb.Model):
    messageId = ndb.StringProperty(indexed=True)
    sentAckd = ndb.DateTimeProperty(indexed=False)