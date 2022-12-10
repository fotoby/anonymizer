import boto3
import json


class S3Facade:

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')

    def write_json_to_s3(self, json_doc: str, s3_key: str) -> None:
        """
        Write a json document to Amazon S3
        :param json_doc: json content to write to S3
        :param s3_key: the key to write to on S3
        :return None
        """
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=json_doc
        )

    def write_dict_as_json_to_s3(self, output_dict: dict, s3_key: str) -> None:
        """
        Write a dict to S3 in a json format
        :param output_dict:
        :param s3_key:
        :return: None
        """
        json_doc = json.dumps(output_dict, indent=2, default=str)
        self.write_json_to_s3(json_doc=json_doc, s3_key=s3_key)

    def read_json_from_s3(self, s3_key: str) -> dict:
        """
        Read S3 object and parse it into JSON format and return is as a dict
        :param s3_key:
        :return: JSON document as a string
        """
        s3_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
        # file_content = s3_object.get()['Body'].read().decode('utf-8')
        file_content = s3_object["Body"].read().decode('utf-8')
        json_content = json.loads(file_content)
        return json_content
