# message_bus

The messaging framework to communicate data across different services.


To run the test.py

1. Please create an AWS client
2. Install boto (pip install boto)
3. Create a .boto file in home folder (vi ~/.boto)
4. Add the text below to the .boto file
[Credentials]
aws_access_key_id = 
aws_secret_access_key = 
5. Run the test.py. This should create queues and the topic. And the queues should have the data published.