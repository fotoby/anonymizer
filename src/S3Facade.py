#######
# DESTINATION_S3 Facade v1.00
#######

import boto3
import json
import datetime
import uuid

ANONYMIZER_MODE = 'ANONYMIZER'
REVERT_MODE = 'REVERT'

ANONYMIZER_ROOT_PATH = "data/anonymized/"
REVERT_ROOT_PATH = "data/reverted/"
DEFAULT_TABLE_NAME = 'default'

s3_client = boto3.client('s3')


class S3Facade:

    def __init__(self, bucket_name, anon_path_root=ANONYMIZER_ROOT_PATH, revert_path_root=REVERT_ROOT_PATH):
        self.anon_path_root = anon_path_root
        self.revert_path_root = revert_path_root
        self.bucket_name = bucket_name

    def write_json_to_s3(self, json_doc: str, s3_key: str) -> None:
        """
        Write a json document to Amazon DESTINATION_S3
        :param json_doc: json content to write to DESTINATION_S3
        :param s3_key: the key to write to on DESTINATION_S3
        :return None
        """
        s3_client.put_object(Body="hello world", Bucket=self.bucket_name, Key="aa")

        """
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=json_doc
        )
        """

    def write_dict_as_json_to_s3(self, output_dict: dict, s3_key: str) -> None:
        """
        Write a dict to DESTINATION_S3 in a json format
        :param output_dict:
        :param s3_key:
        :return: None
        """
        # json_doc = json.dumps(output_dict, indent=2, default=str)
        json_doc = json.dumps(output_dict)
        self.write_json_to_s3(json_doc=json_doc, s3_key=s3_key)

    def read_json_from_s3(self, s3_key: str) -> dict:
        """
        Read DESTINATION_S3 object and parse it into JSON format and return is as a dict
        :param s3_key:
        :return: JSON document as a string
        """
        s3_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
        file_content = s3_object["Body"].read().decode('utf-8')
        json_content = json.loads(file_content)
        return json_content

    def generate_s3_key_for_table(self, table_name: str, mode=None) -> str:
        """
        Create a DESTINATION_S3 key for a table
        Have the table name as the root of dynamic part of the key, followed by the date split by yyyy/mm/dd,
        followed by the hour split by 00, then user last 8 chars from uuid
        :param table_name: the name of the client table being processed
        :param mode: is key for output in ANONYMIZER_MODE or REVERT_MODE
        :return: the key to use for the s3 object that will be created
        """
        if mode is None or mode == REVERT_MODE:
            key = self.revert_path_root
        else:
            key = self.anon_path_root
        # append the table name
        key += f"{table_name}/"
        # add date split by YYYY MM DD HH
        key += datetime.datetime.now().strftime("%Y/%m/%d/%H/")
        # add a random id that should not clash with other runs
        key += str(uuid.uuid4())[-8:]
        return key

    def write_anonymized_records(self, records, table_name: str = DEFAULT_TABLE_NAME) -> str:
        """
        Write anonymized records to DESTINATION_S3
        :param records: the records to write to DESTINATION_S3
        :param table_name: the name of the table being processed
        :return: None
        """
        s3_key = self.generate_s3_key_for_table(table_name=table_name, mode=ANONYMIZER_MODE)
        self.write_dict_as_json_to_s3(output_dict=records, s3_key=s3_key)
        return f"{self.bucket_name}/{s3_key}"

    def write_reverted_records(self, records, table_name: str = DEFAULT_TABLE_NAME) -> None:
        """
        Write reverted records to DESTINATION_S3
        :param records: the records to write to DESTINATION_S3
        :param table_name: the name of the table being processed
        :return: None
        """
        s3_key = self.generate_s3_key_for_table(table_name=table_name, mode=REVERT_MODE)
        self.write_dict_as_json_to_s3(output_dict=records, s3_key=s3_key)
