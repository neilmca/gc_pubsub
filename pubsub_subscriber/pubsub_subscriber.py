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

    subscriptionName = buildFullSubscriptionName(subscription_project, name)
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


    subscriptionName = buildFullSubscriptionName(subscription_project, sname)
    subscription = client.projects().subscriptions().create(
        name=subscriptionName,
        body=body).execute()

    logging.info('Created: %s' % subscription.get('name'))

def subscription_pull_messages(subscription, exclude_filters):
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

    exclude_filters = get_exclude_filter_matched_to_subscription(subscription)
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
                    ProcessMessage(pubsub_message, exclude_filters, subscription)

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

def ProcessMessage(pubsub_message, exclude_filters, subscription):
    # Process messages
    message_id = pubsub_message.get('messageId')
    logging.info('received message id = %s' % message_id)
    mbody = base64.b64decode(str(pubsub_message.get('data')))
    summary = log_summary(mbody)
    mbody_aug = PulledMessages.AugmentLoggedJson(mbody, subscription)

    if exclude_if_matches_filter(summary['status'], exclude_filters) == False:

        #dispatch message
        log_ts = datetime.datetime.strptime(summary['startTime'], "%Y-%m-%dT%H:%M:%S.%fZ")
        ack_ts = datetime.datetime.utcnow()
        pubsubReceiveLag = int((ack_ts - log_ts).total_seconds())
        msg = PulledMessages(messageId = message_id, receivedAckd = ack_ts, body = mbody_aug, log_summary = json.dumps(summary, sort_keys=True, indent=4), subscription = subscription, pubsubReceiveLagSecs = pubsubReceiveLag)
        msg.put()
    else:
        logging.info('not dispatching message = %s as status (%s) is in list of exclude filters' % (message_id, summary['status']))

def log_summary(log_body):
    status = ''
    method = ''
    startTime = ''
    endTime = ''
    resource = ''
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
        if 'resource' in protop:
            resource = protop['resource']
        if 'host' in protop:
            host = protop['host']
     
       

    log_summary = {'status' : status, 'method' : method, 'startTime' : startTime, 'endTime': endTime, 'resource' : resource, 'host' : host}
    return log_summary

def check_and_create_subscriptions():

    logging.info('check_and_create_subscriptions')
    if Subscriptions.IsSubscriptionNameFullySet() == True:
        all = Subscriptions.getSubscriptions()

        for subs_to_create in all:
            topic_project = subs_to_create.topic_project
            subscription_project = getOwningProject()
            topic = subs_to_create.topic_to_subscribe
            subscriptionName = buildSubscriptionName(topic_project, topic)
                
            if not check_subscription_exists(subscription_project, subscriptionName):
                logging.info('creating subscription (%s) to topic %s' % (subscriptionName, buildTopicName(topic_project, topic)))
                create_subscription(subscription_project, topic_project, topic, subscriptionName)


    else:
        logging.info('no subscriptions to set as names not set in Subscriptions datastore')

def get_exclude_filter_matched_to_subscription(fullSubscriptionToMatch):

    #loop through each DS subscriton entry and see if it matches this one
    all = Subscriptions.getSubscriptions()
    for subsDS in all:
        ds_topic_project = subsDS.topic_project
        ds_subscription_project = getOwningProject()
        ds_topic = subsDS.topic_to_subscribe
        subscriptionName = buildSubscriptionName(ds_topic_project, ds_topic)
        fullName = buildFullSubscriptionName(ds_subscription_project, subscriptionName)        
        if fullName == fullSubscriptionToMatch:
            #match
            filters = subsDS.exclude_filters.encode('utf8').split(",")
            return filters

    return []

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

    logging.info('range_filters = %s' % str(range_filters))
    logging.info('explicit_filters = %s' % str(explicit_filters))

    if statusi in explicit_filters:
        return True;

    for start_digit in range_filters:
        if statusi >= start_digit * 100 and statusi < (start_digit+1) * 100:
            return True;

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
          logging.info('CronPullFromTopicHandler invoked pulling from subscription %s' % subscription)

          #get any exclude filters
          exclude_filters = get_exclude_filter_matched_to_subscription(subscription)
          

          subscription_pull_messages(subscription, exclude_filters)     
      

Subscriptions.init()

check_and_create_subscriptions()

logging.getLogger().setLevel(logging.DEBUG)

app = webapp2.WSGIApplication([
    ('/cron_pullfromtopic', CronPullFromTopicHandler),
    ('/.*', PubSubHandler)    
    
], debug=True)


