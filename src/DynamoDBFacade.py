#######
# DynamoDB Facade v1.00
#######

import boto3
import boto3.dynamodb.types
import uuid
from decimal import Decimal

SCRUB_TRANSFORMS_TABLE = 'scrub_transforms'


class DynamoDBFacade:

    def __init__(self, region):
        self.dynamodb_client = boto3.client('dynamodb', region_name=region)
        self.scrub_xform_table_name = SCRUB_TRANSFORMS_TABLE
        self.scrub_xform_table = boto3.resource('dynamodb', region_name=region).Table(self.scrub_xform_table_name)
        self.deserializer = boto3.dynamodb.types.TypeDeserializer()

    @staticmethod
    def create_base64_guid() -> str:
        """
        Creates a base64 guid
        """
        return str(uuid.uuid4())

    def get_scrub_xform(self, guid: str) -> dict:
        """
        Gets a scrub transform by guid from the ddb table
        """
        response = self.scrub_xform_table.get_item(Key={'guid': guid})
        item = response['Item']

        # Convert DynamoDB Decimals back to int
        transforms = []
        for xform in item['Transforms']:
            # convert Decimal type to int
            xform = dict(map(lambda x: (x[0], int(x[1])) if isinstance(x[1], Decimal) else x, xform.items()))
            transforms.append(xform)

        return {'Transforms': transforms}

    def put_scrub_xform(self, scrub_xform: dict) -> str:
        """
        Puts a scrub transform to the ddb table
        """
        key = self.create_base64_guid()
        scrub_xform['guid'] = key
        self.scrub_xform_table.put_item(Item=scrub_xform)
        return key
