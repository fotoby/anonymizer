#######
# Inference Facade v1.00
#######

import boto3
import json

# This maps to the sagemaker deployed endpoint name
END_POINT_FR = "pii-fr-e-endpoint"


class InferenceFacade:

    def __init__(self, language_code='en'):
        self.language_code = language_code
        if language_code == 'fr':
            self.end_point_name = END_POINT_FR
            self.end_point_client = boto3.client('sagemaker-runtime')
            self.ai_detect_facade = self.detect_pii_entities_fr
            self.start_offset = 'start'
            self.end_offset = 'end'
            self.type_keyword = 'entity_type'
        else:
            self.comprehend_client = boto3.client('comprehend')
            self.ai_detect_facade = self.detect_pii_entities_en
            self.start_offset = 'BeginOffset'
            self.end_offset = 'EndOffset'
            self.type_keyword = 'Type'

    def detect_pii_entities(self, text: str) -> dict:
        """
        Get detail on PII entities in the given text
        :param text: text to detect PII entities
        :return: dict of PII entities with details on their location in the text
        """
        entities = self.ai_detect_facade(text)
        transform_list = []
        for entity in entities:
            transform = {'Type': entity[self.type_keyword],
                         'BeginOffset': entity[self.start_offset],
                         'EndOffset': entity[self.end_offset],
                         'Original': "",
                         'Anonymized': ""
                         }
            transform_list.append(transform)
        transforms = {'Transforms': transform_list}
        return transforms

    def detect_pii_entities_en(self, text: str) -> list:
        """
        Get detail on PII entities in the given text
        :param text: text to detect PII entities
        :return: dict of PII entities with details on their location in the text
        """
        response = self.comprehend_client.detect_pii_entities(Text=text, LanguageCode='en')
        return response['Entities']

    def detect_pii_entities_fr(self, text: str) -> list:
        """
        Get detail on PII and sensitive entities in the given text
        There is ad hoc adjustments for the returned entities to make it better match with Comprehend service
        :param text: text to detect PII entities
        :return: list of PII entities with details on their location in the text
        """
        data = {
            "inputs": text,
            "parameters": {
                "entities": ["PERSON", "ORGANIZATION", "EMAIL_ADDRESS", "PHONE_NUMBER", "LOCATION"]
            }
        }
        binary_payload = bytes(json.dumps(data), 'utf-8')
        response = self.end_point_client.invoke_endpoint(EndpointName=self.end_point_name,
                                                         ContentType='application/json',
                                                         Body=binary_payload)
        body = response['Body'].read().decode('utf-8')
        body = json.loads(body)
        # order the entities  by start offset
        entities = body['found']
        entities.sort(key=lambda x: x['start'])
        # remove entities with score less than 0.8
        entities = [entity for entity in entities if entity['score'] > 0.8 or entity['entity_type'] == 'PHONE_NUMBER']
        # TODO: Filter over lapping entities if they occur
        for entity in entities:
            if entity['entity_type'] not in ['PHONE_NUMBER', 'EMAIL_ADDRESS']:
                entity['start'] = entity['start'] + 1
            if entity['entity_type'] == 'PHONE_NUMBER':
                entity['entity_type'] = 'PHONE_NUMBER_FR'
            if entity['entity_type'] == 'PERSON':
                entity['entity_type'] = 'PERSON_FR'
        return entities






