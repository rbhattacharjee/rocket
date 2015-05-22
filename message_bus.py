import os


import json
import boto.sqs, boto.sns
from boto.sqs.message import RawMessage
from S3_Manager import S3_Manager

'''
MessageBus provides a messaging framework across services.
Currently messaging infrastructure is built on amazon queuing and notification service
'''

class MessageBus():

    def __init__(self, queue_name,connect_to_region="us-east-1", topic_name=None):
        '''
        :param queue_name: String (Mandatory parameter) The queue name
        :param connect_to_region: String (Optional parameter, default value us-west2) Amazon AZ name.
        :param topic_name: String (optional parameter, default value None) Topic Name. If no topic name is passed then data is directly
        written to the queue else data is always published via SNS.
        :return queue_object, topic_object and some other attributes are attached to the MessageBus object.
        '''
        self.connect_to_region = connect_to_region
        print self.connect_to_region
        self.topic_name = topic_name
        self.queue_object = None
        self.topic_object = None
        self.sns_topic_arn = None
        self.subscription_arn = None
        self.s3object = S3_Manager("messagebusdata")

        # Create the sqs connection object and queue object
        self.sqs_conn = boto.sqs.connect_to_region(self.connect_to_region)
        self.queue_object = self.sqs_conn.create_queue(queue_name)
        print("SQS queue \"%s\" created successfully "% self.queue_object.name)

        if self.topic_name is not None:
            # Create the SNS connection object and the SNS topic object
            self.sns_conn = boto.sns.connect_to_region(connect_to_region)
            self.topic_object = self.sns_conn.create_topic(self.topic_name)
            self.sns_topic_arn = self.topic_object['CreateTopicResponse']['CreateTopicResult']['TopicArn']
            #self.topic_object.set_topic_attributes(self.sns_topic_arn,"DisplayName", "foo")
            print("SNS topic created, ARN of the topic is : %s" % self.sns_topic_arn)
            subscription_result = self.sns_conn.subscribe_sqs_queue(self.sns_topic_arn, self.queue_object)
            self.subscription_arn = subscription_result['SubscribeResponse']['SubscribeResult']['SubscriptionArn']
            print("Subscription successful, subscription_arn %s" % self.subscription_arn)

    def write_data(self, input_data):
        '''
        Writes a data to the queue created, or publishes the data via a topic to the queues
        :param input_data: Data to be written to the queue, published to queues via a topic

        '''
        producer_data = json.dumps(input_data)
        matcher =  json.loads(producer_data)

        if matcher[0]['meta-inf'] == "content":
            if self.topic_name is not None:
                print("Publishing the data to the topic object")
                publication = self.sns_conn.publish(self.sns_topic_arn,producer_data)
            else:
                print("Writing the data to the queue")
                queue_data = RawMessage()
                queue_data.set_body(json.dumps(input_data))
                # Writing the data to the queue
                self.queue_object.write(queue_data)
        elif matcher[0]['meta-inf'] == "meta":
            # S3 content goes here
            raw_data = matcher[1]['data']
            file_key = os.path.split(raw_data)[-1]
            self.s3object.upload(file_key,raw_data)
            if self.topic_name is not None:
                print("Publishing the data to the topic object")
                publication = self.sns_conn.publish(self.sns_topic_arn,producer_data)
            else:
                print("Writing the data to the queue")
                queue_data = RawMessage()
                queue_data.set_body(json.dumps(input_data))
                # Writing the data to the queue
                self.queue_object.write(queue_data)




    def read_data(self):
        '''
        Read the data from the queue, and also preserves the data-type.
        :return: Result set if data is available or 0 if no data is present in the queue.
        '''
        result_set = self.queue_object.get_messages()
        if result_set:
            if self.topic_name is not None:
                data_and_metadata= json.loads(result_set[0].get_body())
                content = json.loads(data_and_metadata['Message'])
                if content[0]['meta-inf']=="content":
                    read_data= content[1]['data']
                    return read_data
                elif content[0]['meta-inf']=="meta":
                    file_path= content[1]['data']
                    file_key = os.path.split(file_path)[-1]
                    download_path =self.s3object.download(file_key)
                    return download_path
            else:
                read_data = json.loads(result_set[0].get_body())
                return read_data[1]['data']
        else:
            return 0
