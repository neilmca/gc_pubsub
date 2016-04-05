from google.appengine.ext import ndb
import logging



class Subscriptions(ndb.Model):
    topic_to_subscribe = ndb.StringProperty(indexed=False)
    topic_project = ndb.StringProperty(indexed=False)

    @staticmethod
    def getSubscriptions():
        all = Subscriptions.query().fetch()
        return all

    @staticmethod
    def IsSubscriptionNameFullySet():
        subs = Subscriptions.getSubscriptions()

        if subs == None:
            return False;
        elif len(subs) > 0 and subs[0].topic_to_subscribe != 'enter name':
            return True;
        else:
            return False;



    @staticmethod
    def init():
        logging.info('Subscriptions.init')
        subs = Subscriptions.getSubscriptions()
        if subs == None or len(subs) == 0:
            ds = Subscriptions(topic_project = 'enter name', topic_to_subscribe = 'enter name')
            ds.put()

    



