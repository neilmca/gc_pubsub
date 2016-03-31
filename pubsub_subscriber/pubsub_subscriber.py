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


#######
#NOTE: This does not work on devserver
#######

PUBSUB_SCOPES = ['https://www.googleapis.com/auth/pubsub']


def getProject():
    return os.environ['PROJECT_CONTAINING_TOPIC']

def getTopicToSubscribeTo():
    return os.environ['TOPIC_TO_SUBSCRIBE_TO']

def getSubscriptionName():
    return os.environ['SUBSCRIPTION']



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




def list_subscriptions(project):
    client = create_pubsub_client()

    subscriptionsList = []

    next_page_token = None
    while True:
        resp = client.projects().subscriptions().list(
            project=buildProjectName(project),
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

def check_subscription_exists(project, name):
    s_list = list_subscriptions(project)

    subscriptionName = buildSubscriptionName(project, name)
    logging.info('check_subscription_exists = %s,  s_list = %s' % (subscriptionName, s_list))

    if subscriptionName in s_list:
        return True

    return False


def create_subscription(project, topic, sname):
    client = create_pubsub_client()
    
    

    # Create a POST body for the Pub/Sub request
    body = {
        # The name of the topic from which this subscription receives messages
        'topic': buildTopicName(project, topic)
    }


    subscriptionName = buildSubscriptionName(project, sname)
    subscription = client.projects().subscriptions().create(
        name=subscriptionName,
        body=body).execute()

    logging.info('Created: %s' % subscription.get('name'))

def subscription_pull_messages(project, sname):
    client = create_pubsub_client()

    # You can fetch multiple messages with a single API call.
    batch_size = 100

    subscription = buildSubscriptionName(project, sname)

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
                    msg = PulledMessages(messageId = message_id, receivedAckd = datetime.datetime.utcnow(), body = mbody)
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

        project = getProject()
        topic = getTopicToSubscribeTo()
        subscriptionName = getSubscriptionName()

        if not check_subscription_exists(project, subscriptionName):
            create_subscription(project, topic, subscriptionName)

        subscriptions = list_subscriptions(project)

        resp = 'ROLE: Subscriber   '
        resp += 'subscriptions = %s' % str(subscriptions)
        self.response.write(resp)

  
 
class CronPullFromTopicHandler(BaseHandler):

    
   
    def get(self):

      logging.info('CronPullFromTopicHandler invoked pulling from subscription %s' % getSubscriptionName())
      subscription_pull_messages(getProject(), getSubscriptionName())     
      


logging.getLogger().setLevel(logging.DEBUG)

app = webapp2.WSGIApplication([
    ('/cron_pullfromtopic', CronPullFromTopicHandler),
    ('/.*', PubSubHandler)    
    
], debug=True)

