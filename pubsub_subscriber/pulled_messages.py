from google.appengine.ext import ndb

class PulledMessages(ndb.Model):
    messageId = ndb.StringProperty(indexed=True)
    receivedAckd = ndb.DateTimeProperty(indexed=False)
    body = ndb.StringProperty(indexed=False)