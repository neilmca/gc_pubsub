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
import os
from published_messages import PublishedMessages
from message_counter import MessageCounter

#######
#NOTE: This does not work on devserver
#######

PUBSUB_SCOPES = ['https://www.googleapis.com/auth/pubsub']


def getProject():
    return os.environ['PROJECT']

def getTopic():
    return os.environ['TOPIC']


def buildTopicName(project, topic):
    return 'projects/%s/topics/%s' % (project, topic)

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

def list_topic(project):

    client = create_pubsub_client()

    topics = []

    next_page_token = None
    while True:
        resp = client.projects().topics().list(
            project=buildProjectName(project),
            pageToken=next_page_token).execute()

        logging.info(str(resp))

        
        # Process each topic
        for topic in resp['topics']:
            logging.info(topic)
            topics.append(topic['name'])
        next_page_token = resp.get('nextPageToken')
        if not next_page_token:
            break

    return topics

def create_topic(project, name):
    logging.info('create topic %s' % name)
    create_pubsub_client().projects().topics().create(
        name=buildTopicName(project, name), body={}).execute()

def check_topic_exists(project, name):
    t_list = list_topic(project)

    topicName=buildTopicName(project, name)
    logging.info('check_topic_exists topic = %s,  t_list = %s' % (topicName, t_list))

    
    if topicName in t_list:
        return True

    return False



def publish_to_topic(project, topic):
    topicName=buildTopicName(project, topic)
    logging.info('publish_to_topic invoked publishing to topic %s' % topicName)

    client = create_pubsub_client()

    # You need to base64-encode your message.

    cursor = MessageCounter.getCounter()
    if cursor == -1:
        MessageCounter.writeNewCounter(0)
        cursor = 0

    message1Body = 'Hello Cloud Pub/Sub! idx=%d %s' % (cursor+1, datetime.datetime.utcnow())
    message2Body = 'We are on the same boat. idx=%d  %s' % (cursor+2, datetime.datetime.utcnow())

    message1 = base64.b64encode(message1Body)
    message2 = base64.b64encode(message2Body)

    # Create a POST body for the Pub/Sub request
    body = {
        'messages': [
            {'data': message1},
            {'data': message2},
        ]
    }

    
    resp = client.projects().topics().publish(
        topic=topicName, body=body).execute()

    newcursor = cursor
    message_ids = resp.get('messageIds')
    if message_ids:
        for message_id in message_ids:
            # Process each message ID and ave in DS
            logging.info('message id = %s' % message_id)
            msg = PublishedMessages(messageId = message_id, sentAckd = datetime.datetime.utcnow())
            msg.put()
            newcursor = newcursor + 1

    if newcursor != cursor:
        MessageCounter.writeNewCounter(newcursor)


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
        topic = getTopic()
       
        if not check_topic_exists(project, topic):
            create_topic(project, topic)

        topics = list_topic(project)
       
        resp = 'ROLE: Publisher   '
        resp += 'topics = %s' % str(topics)
        self.response.write(resp)

  
class CronPublishToTopicHandler(BaseHandler):


    
    def get(self):
        logging.info('CronPublishToTopicHandler invoked publishing to topic %s' % getTopic())
        publish_to_topic(getProject(), getTopic())   
        
   


logging.getLogger().setLevel(logging.DEBUG)

app = webapp2.WSGIApplication([
    ('/cron_publishtotopic', CronPublishToTopicHandler),
    ('/.*', PubSubHandler)    
    
], debug=True)

