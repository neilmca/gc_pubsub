from google.appengine.api import users
import webapp2
import logging
import base64
# google data store access
from google.appengine.ext import ndb
import re
import datetime
import random
import string
import httplib2
from apiclient import discovery
from oauth2client import client as oauth2client
from pulled_messages import PulledMessages
import os
from google.appengine.api.app_identity import get_application_id
from subscriptions import Subscriptions


#######
#NOTE: This does not work on devserver
#######

PUBSUB_SCOPES = ['https://www.googleapis.com/auth/pubsub']

def getOwningProject():
    return get_application_id()

def buildTopicName(project, topic):
    return 'projects/%s/topics/%s' % (project, topic)

def buildSubscriptionName(project, subscription):
    return 'projects/%s/subscriptions/%s' % (project, subscription)

def buildProjectName(project):
    return 'projects/%s' % project



def create_pubsub_client(http=None):
    logging.info('create_pubsub_client')
    credentials = oauth2client.GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(PUBSUB_SCOPES)
    if not http:
        http = httplib2.Http()
    credentials.authorize(http)

    return discovery.build('pubsub', 'v1', http=http)

client = create_pubsub_client()




def list_subscriptions(subscription_project):
    client = create_pubsub_client()

    subscriptionsList = []

    next_page_token = None
    while True:
        resp = client.projects().subscriptions().list(
            project=buildProjectName(subscription_project),
            pageToken=next_page_token).execute()

        # Process each subscription
        if 'subscriptions' in resp:
            for subscription in resp['subscriptions']:
                logging.info(subscription)
                subscriptionsList.append(subscription['name'])
            next_page_token = resp.get('nextPageToken')
            if not next_page_token:
                break
        else:
            logging.info('subscriptions key not in resp %s' % str(resp))
            break;

    return subscriptionsList

def check_subscription_exists(subscription_project, name):
    s_list = list_subscriptions(subscription_project)

    subscriptionName = buildSubscriptionName(subscription_project, name)
    logging.info('check_subscription_exists = %s,  s_list = %s' % (subscriptionName, s_list))

    if subscriptionName in s_list:
        return True

    return False


def create_subscription(subscription_project, topic_project, topic, sname):

    #subscription is owned by this project but can subscribe to a topic in aonther project

    client = create_pubsub_client()
    
    

    # Create a POST body for the Pub/Sub request
    body = {
        # The name of the topic from which this subscription receives messages
        'topic': buildTopicName(topic_project, topic)
    }


    subscriptionName = buildSubscriptionName(subscription_project, sname)
    subscription = client.projects().subscriptions().create(
        name=subscriptionName,
        body=body).execute()

    logging.info('Created: %s' % subscription.get('name'))

def subscription_pull_messages(subscription):
    client = create_pubsub_client()

    # You can fetch multiple messages with a single API call.
    batch_size = 100

    # Create a POST body for the Pub/Sub request
    body = {
        # Setting ReturnImmediately to false instructs the API to wait
        # to collect the message up to the size of MaxEvents, or until
        # the timeout.
        'returnImmediately': True,
        'maxMessages': batch_size,
    }

    while True:

        resp = client.projects().subscriptions().pull(
            subscription=subscription, body=body).execute()

        received_messages = resp.get('receivedMessages')
        if received_messages is not None:
            ack_ids = []
            for received_message in received_messages:
                pubsub_message = received_message.get('message')
                if pubsub_message:
                    # Process messages
                    message_id = pubsub_message.get('messageId')
                    logging.info('received message id = %s' % message_id)
                    mbody = base64.b64decode(str(pubsub_message.get('data')))

                    mbody_aug = PulledMessages.AugmentLoggedJson(mbody, subscription)
                    msg = PulledMessages(messageId = message_id, receivedAckd = datetime.datetime.utcnow(), body = mbody_aug, subscription = subscription)
                    msg.put()

                    # Get the message's ack ID
                    ack_ids.append(received_message.get('ackId'))

            # Create a POST body for the acknowledge request
            ack_body = {'ackIds': ack_ids}

            # Acknowledge the message.
            client.projects().subscriptions().acknowledge(
                subscription=subscription, body=ack_body).execute()
        else:
            logging.info('received_messages is none')
            break;


def check_and_create_subscriptions():

    logging.info('check_and_create_subscriptions')
    if Subscriptions.IsSubscriptionNameFullySet() == True:
        all = Subscriptions.getSubscriptions()

        for subs_to_create in all:
            topic_project = subs_to_create.topic_project
            subscription_project = getOwningProject()
            topic = subs_to_create.topic_to_subscribe
            subscriptionName = topic_project + '_' + topic
                
            if not check_subscription_exists(subscription_project, subscriptionName):
                logging.info('creating subscription (%s) to topic %s' % (subscriptionName, buildTopicName(topic_project, topic)))
                create_subscription(subscription_project, topic_project, topic, subscriptionName)


    else:
        logging.info('no subscriptions to set as names not set in Subscriptions datastore')

    

class BaseHandler(webapp2.RequestHandler):
    def handle_exception(self, exception, debug):
        # Log the error.
        
        logging.exception(exception)

        # Set a custom message.
        self.response.write('An error occurred.')

        # If the exception is a HTTPException, use its error code.
        # Otherwise use a generic 500 error code.
        if isinstance(exception, webapp2.HTTPException):
            self.response.set_status(exception.code)
        else:
            self.response.set_status(500)





class PubSubHandler(BaseHandler):

   
    def get(self):
       
        subscription_project = getOwningProject()      
        subscriptions = list_subscriptions(subscription_project)

        resp = '%s ROLE: Subscriber   ' % getOwningProject()
        resp += 'subscriptions = %s' % str(subscriptions)
        self.response.write(resp)

  
 
class CronPullFromTopicHandler(BaseHandler):

    
   
    def get(self):

      check_and_create_subscriptions()

      subscription_project = getOwningProject()
      subscriptions = list_subscriptions(subscription_project)

      for subscription in subscriptions:
          logging.info('CronPullFromTopicHandler invoked pulling from subscription %s' % subscription)
          subscription_pull_messages(subscription)     
      

Subscriptions.init()

check_and_create_subscriptions()

logging.getLogger().setLevel(logging.DEBUG)

app = webapp2.WSGIApplication([
    ('/cron_pullfromtopic', CronPullFromTopicHandler),
    ('/.*', PubSubHandler)    
    
], debug=True)

