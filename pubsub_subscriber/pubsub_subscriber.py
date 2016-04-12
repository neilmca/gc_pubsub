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
import json
import math
import sys

#######
#NOTE: This does not work on devserver
#######

PUBSUB_SCOPES = ['https://www.googleapis.com/auth/pubsub']

def getOwningProject():
    return get_application_id()

def buildTopicName(project, topic):
    return 'projects/%s/topics/%s' % (project, topic)

def buildFullSubscriptionName(project, subscription):
    return 'projects/%s/subscriptions/%s' % (project, subscription)

def buildProjectName(project):
    return 'projects/%s' % project

def buildSubscriptionName(project, topic):
    return project + '___' + topic

def buildDestinationTopic(dest_project, dest_topic):
    return 'projects/%s/topics/%s' % (dest_project, dest_topic)





def create_pubsub_client(http=None):
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
                subscriptionsList.append(subscription['name'])
            next_page_token = resp.get('nextPageToken')
            if not next_page_token:
                break
        else:
            break;

    return subscriptionsList

def check_subscription_exists(subscription_project, name):
    s_list = list_subscriptions(subscription_project)

    subscriptionName = buildFullSubscriptionName(subscription_project, name)

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


    subscriptionName = buildFullSubscriptionName(subscription_project, sname)
    subscription = client.projects().subscriptions().create(
        name=subscriptionName,
        body=body).execute()

    logging.info('Created subscription: %s' % subscription.get('name'))

def subscription_pull_messages(subscription, exclude_filters, destination_topic):
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

    logging.info('exclude filters for subscription = %s'% str(exclude_filters))
    while True:

        resp = client.projects().subscriptions().pull(
            subscription=subscription, body=body).execute()

        received_messages = resp.get('receivedMessages')
        if received_messages is not None:
            ack_ids = []
            for received_message in received_messages:
                pubsub_message = received_message.get('message')
                if pubsub_message:
                    if ProcessMessage(pubsub_message, exclude_filters, subscription, destination_topic, client) == True:
                        # ack the message if it was dispatched ok
                        ack_ids.append(received_message.get('ackId'))

            if len(ack_ids) > 0:
                # Create a POST body for the acknowledge request
                ack_body = {'ackIds': ack_ids}

                # Acknowledge the message.
                client.projects().subscriptions().acknowledge(
                    subscription=subscription, body=ack_body).execute()
        else:
            logging.info('received_messages is none')
            break;

def ProcessMessage(pubsub_message, exclude_filters, subscription, destination_topic, client):
    # Process messages
    message_id = pubsub_message.get('messageId')
    mbody = base64.b64decode(str(pubsub_message.get('data')))
    summary = log_summary(mbody)    

    if exclude_if_matches_filter(summary['status'], exclude_filters) == False:

        #dispatch message
        logging.info('dispatching message = %s' % json.dumps(summary, sort_keys=True, indent=4))
        log_ts = datetime.datetime.strptime(summary['startTime'], "%Y-%m-%dT%H:%M:%S.%fZ")
        ack_ts = datetime.datetime.utcnow()
        pubsubReceiveLag = int((ack_ts - log_ts).total_seconds())

        #publish to destination topic
        #uncomment after topic set up by Oleksii
        sent = True
        #sent = publish_log_to_destination_topic(mbody, destination_topic, client)

        #write to DS
        mbody_aug = PulledMessages.AugmentLoggedJson(mbody, subscription)
        msg = PulledMessages(messageId = message_id, receivedAckd = ack_ts, body = mbody_aug, log_summary = json.dumps(summary, sort_keys=True, indent=4), subscription = subscription, pubsubReceiveLagSecs = pubsubReceiveLag)
        msg.put()

        return sent
    else:
        logging.info('not dispatching message from host (%s) as status (%s) is in list of exclude filters' % (summary['host'], summary['status']))
        return True

def log_summary(log_body):
    status = ''
    method = ''
    startTime = ''
    endTime = ''
    host = ''

    logJson = json.loads(log_body)   
    if 'protoPayload' in logJson:
        protop = logJson['protoPayload']
        if 'status' in protop:
            status = protop['status']
        if 'method' in protop:
            method = protop['method']
        if 'startTime' in protop:
            startTime = protop['startTime']
        if 'endTime' in protop:
            endTime = protop['endTime']
        if 'host' in protop:
            host = protop['host']

    log_summary = {'status' : status, 'method' : method, 'startTime' : startTime, 'endTime': endTime, 'host' : host}
    return log_summary

def check_and_create_subscriptions():

    if Subscriptions.IsSubscriptionNameFullySet() == True:
        all = Subscriptions.getSubscriptions()

        for subs_to_create in all:
            topic_project = subs_to_create.topic_project
            subscription_project = getOwningProject()
            topic = subs_to_create.topic_to_subscribe
            subscriptionName = buildSubscriptionName(topic_project, topic)
                
            if not check_subscription_exists(subscription_project, subscriptionName):
                
                create_subscription(subscription_project, topic_project, topic, subscriptionName)


    else:
        logging.info('no subscriptions to set as names not set in Subscriptions datastore')


def get_subscription_dsentry_matched_to_subscription(fullSubscriptionToMatch):
    all = Subscriptions.getSubscriptions()
    for subsDS in all:
        ds_topic_project = subsDS.topic_project
        ds_subscription_project = getOwningProject()
        ds_topic = subsDS.topic_to_subscribe
        subscriptionName = buildSubscriptionName(ds_topic_project, ds_topic)
        fullName = buildFullSubscriptionName(ds_subscription_project, subscriptionName)        
        if fullName == fullSubscriptionToMatch:
            #match
            return subsDS

    return None

def exclude_if_matches_filter(status, filters):

    #filters can be explicit or a range example = 201, 2xx, 3xx, 

    try:
        statusi = int(status)
    except TypeError:
        return False    
   

    range_filters = []
    explicit_filters = []
    for filter in filters:
        if 'xx' in filter:
            digit = filter.replace('xx','')
            try:
                range_filters.append(int(digit))
            except:
                pass            
        else:
            try:
                explicit_filters.append(int(filter))
            except:
                pass             

    if statusi in explicit_filters:
        return True;

    for start_digit in range_filters:
        if statusi >= start_digit * 100 and statusi < (start_digit+1) * 100:
            return True;

    return False


def publish_log_to_destination_topic(data, destination_topic, client):
    msg = base64.urlsafe_b64encode(data)
    body = {
        'messages': [
            {'data': msg},
        ]
    }

    try:
        resp = client.projects().topics().publish(
            topic=destination_topic, body=body).execute()

        message_ids = resp.get('messageIds')
        if message_ids:
            return True
    except:
        e = sys.exc_info()[0]
        logging.info('failed to send message to destination topic %s error = %s' % (destination_topic, str(e)))
        pass

    
    return False




class BaseHandler(webapp2.RequestHandler):
    def handle_exception(self, exception, debug):
        # Log the error.
        
        logging.exception(exception)

        # Set a custom message.
        self.response.write('An error occurred. %s' % str(exception))

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

        resp = '<p>GAE Logs Subscriber'
        resp += '<p style="text-indent: 2em;">Owning project: %s' % getOwningProject()
        resp += '<p style="text-indent: 2em;">Role: Subscriber   . Subscriptions....'

        for subs in subscriptions:
             resp += '<p style="text-indent: 4em;">%s' % subs

        resp += '<p style="text-indent: 2em;">'
        resp += '<p style="text-indent: 2em;">'
        


        self.response.write(resp)
        


  
 
class CronPullFromTopicHandler(BaseHandler):

    
   
    def get(self):

      check_and_create_subscriptions()

      subscription_project = getOwningProject()
      subscriptions = list_subscriptions(subscription_project)

      for subscription in subscriptions:
          logging.info('CronPullFromTopicHandler invoked pull from subscription %s' % subscription)

          #find the subscription details for this subscription in the DS
          subsDS = get_subscription_dsentry_matched_to_subscription(subscription)
          
          exclude_filters = ''
          if subsDS !=None:
            #get any exclude filters
            exclude_filters = subsDS.exclude_filters.encode('utf8').split(",")
            #get destination topic
            destination_topic = buildDestinationTopic(subsDS.destination_project, subsDS.destination_topic)
            #process messages for this subscription
            subscription_pull_messages(subscription, exclude_filters, destination_topic)  
          else:
            logging.info('Could not find entry in DS matching subscription %s' % subscription)
      

Subscriptions.init()

check_and_create_subscriptions()

logging.getLogger().setLevel(logging.DEBUG)

app = webapp2.WSGIApplication([
    ('/cron_pullfromtopic', CronPullFromTopicHandler),
    ('/.*', PubSubHandler)    
    
], debug=True)


