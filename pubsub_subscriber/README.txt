Install packages

# mkdir lib
# pip install -r requirements.txt -t lib


#the subscriber project must be granted the roles/pubsub.editor by the project containing the topic thatis published
#DO this in Permissions using the subscriber projects default appengine service account email

topic_project = mq-cloud-prototyping-2
topic = gae_logs or topic2
subscription_name = fromProto1ToGaeLogs or fromProto1ToTopic2

