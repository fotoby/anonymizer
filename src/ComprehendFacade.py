from typing import Final
import boto3


class ComprehendFacade:

    EMAIL_KEY: Final[str] = 'EMAIL'
    NAME_KEY: Final[str] = 'NAME'
    PHONE_KEY: Final[str] = 'PHONE'

    in_scope_entity_types = [EMAIL_KEY, NAME_KEY, PHONE_KEY]

    def __init__(self):
        self.comprehend_client = boto3.client('comprehend')

    def set_api_output(self, api_output: dict) -> None:
        """
        Initialize instance with the result from calling Comprehend
        :param self: 
        :param api_output: the output from Comprehend API
        :return: None
        """
        self.api_output = api_output
        return

    def get_pii_base_transforms(self, text: str) -> dict:
        """
        Get the PII entities in scope from the result dict
        :return:
        """
        comprehend_result = self.comprehend_client.detect_pii_entities(Text=text, LanguageCode='en')
        transform_list = []
        for entity in comprehend_result['Entities']:
            transform = {'Type': entity['Type'],
                         'BeginOffset': entity['BeginOffset'],
                         'EndOffset': entity['EndOffset'],
                         'Original': "original TBD",
                         'Anonymized': "anonymized TBD"
                         }
            transform_list.append(transform)
        transforms = {'Transforms': transform_list}
        return transforms





