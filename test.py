__author__ = 'rohit'

from message_bus import MessageBus

def test():


    # Use Case 1: Queue publishes data via the topic

    print "-"*80
    print "Use Case 1: Producer publishes data via using a topic (Pub-Sub model) "
    print "-"*80
    queue_with_topic = MessageBus(queue_name="queue_with_topic" , topic_name="chicken")
    input_data_use_case_1 =  [{'meta-inf':'content'},
                  {'data':[{'campaign_name':"Ford Campaign",'message':"suggestions ready"}]}]
    print "Writing the input data"
    print input_data_use_case_1
    queue_with_topic.write_data(input_data_use_case_1)
    print"Reading data from the queue"
    print queue_with_topic.read_data()
    print "-"*80

    # Use Case 2: Data is published directly via the queue

    print "-"*80
    print "Use Case 2: Producer publishes data directly to a queue "
    print "-"*80
    queue_push = MessageBus( queue_name="queue_direct_push", connect_to_region="ap-southeast-1")

    input_data_use_case_2 = [{'meta-inf':'content'},
                 {'data':[{'campaign_name':"Campaign 150",'advertiser_name':"Advertiser 250"}]}]

    print "Writing the input data"
    print input_data_use_case_2
    queue_push.write_data(input_data_use_case_2)
    data = queue_push.read_data()
    print"Reading data from the queue"
    print data
    print "-"*80

    # Use Case 3

    print "-"*80
    print "Use Case 3: Producer publishes meta-data information to a topic and the data is uploaded to S3 "
    print "-"*80
    queue_with_meta_content = MessageBus(queue_name="fileQueue" , topic_name="images")
    input_data_use_case_2 = [{'meta-inf':'meta'},
                 {'data': "/Users/rohit/Desktop/Screen Shot 2015-05-19 at 11.44.07 am.png"}]

    print "Writing the input data"
    print input_data_use_case_2
    queue_with_meta_content.write_data(input_data_use_case_2)
    print"Reading data from the queue"
    print queue_with_meta_content.read_data()

if __name__=='__main__':
    test()