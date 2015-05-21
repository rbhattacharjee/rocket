


import boto
import os

class S3_Manager(object):

    def __init__(self, bucket):
        self.s3_connection = boto.connect_s3()
        self.bucket = self.__get_bucket(bucket)
        self.local_path = '/tmp/s3/'
        self.__create_dir()

    def __create_dir(self):
        if not os.path.exists(self.local_path):
            os.makedirs(self.local_path)


    def __get_bucket(self, bucket):
        for b in self.s3_connection.get_all_buckets():
            if b.name == bucket:
                return b

        else:
            return self.s3_connection.create_bucket(bucket)


    def upload(self, filekey, filepath):
        key = self.bucket.new_key(filekey)
        key.set_contents_from_filename(filepath)


    def download(self, filekey):
        key = self.bucket.get_key(filekey)
        download_path = os.path.join(self.local_path, filekey)
        key.get_contents_to_filename(download_path)
        return download_path
