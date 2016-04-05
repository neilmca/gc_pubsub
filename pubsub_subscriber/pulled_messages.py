from google.appengine.ext import ndb
import datetime
import json
import logging

class PulledMessages(ndb.Model):
    messageId = ndb.StringProperty(indexed=True)
    receivedAckd = ndb.DateTimeProperty(indexed=False)
    body = ndb.StringProperty(indexed=False)
    subscription = ndb.StringProperty(indexed=True)



    @staticmethod
    def AugmentLoggedJson(loggedJson, subscription):

    	jsonLogObj = json.loads(loggedJson)

        #augment with application name & timestamp

        responseJson = {'timestamp' : str(datetime.datetime.utcnow()), 'subscription': subscription, 'log_message' : jsonLogObj}
        return json.dumps(responseJson, sort_keys=True, indent=4)



