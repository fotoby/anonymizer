from typing import Final


class PiiAnonymizer(object):

    def __init__(self, lang='en'):
        self.lang = lang

    EMAIL_KEY: Final[str] = 'EMAIL'
    NAME_KEY: Final[str] = 'NAME'
    PHONE_KEY: Final[str] = 'PHONE'

    @staticmethod
    def anonymize_email(email_address: str) -> str:
        """
        Anonymize email address
        :param email_address: email address
        :return: anonymized email address
        """
        anonymize_email_account_id: str = 'anon'
        anonymize_email_domain: str = '@anon.com'

        return anonymize_email_account_id + anonymize_email_domain

    @staticmethod
    def anonymize_phone_number(phone_number: str) -> str:
        """
        Anonymize phone number
        :param phone_number: phone number
        :return: anonymous phone number
        """
        anon_phone = ''.join(filter(str.isdigit, phone_number))
        anon_phone = anon_phone[0:3]
        return "(" + anon_phone + ")" + " 111-1111"

    @staticmethod
    def anonymize_name(name: str) -> str:
        """
        Anonymize name
        :param name: name
        :return: anonymized name
        """
        default_first_name = 'John'
        default_last_name = 'Doe'

        # count words in the name
        word_count = len(name.split())
        if word_count == 1:
            return default_first_name
        else:
            return default_first_name + ' ' + default_last_name

    @staticmethod
    def replace_string_at_offset(text: str, start_offset: int, end_offset, replacement: str) -> str:
        """
        Replace string at offset
        :param text: the original text
        :param start_offset: the start offset
        :param end_offset: the end offset
        :param replacement: the replacement string
        :return: the new text
        """
        return text[:start_offset] + replacement + text[end_offset:]

    @staticmethod
    def get_sub_string_from_text(text: str, start_offset: int, end_offset: int) -> str:
        """
        Get sub string from string
        :param text: the original text
        :param start_offset: the start offset
        :param end_offset: the end offset
        :return: the sub string
        """
        return text[start_offset:end_offset+1]

    def anonymize_text(self, text: str, transform_as_dict: dict) -> dict:
        """
        Anonymize text
        :param text: the original text
        :param transform_as_dict: the transform as dict
        :return: the anonymized text
        """

        for transform in transform_as_dict['Transforms']:
            word_to_anonymize = self.get_sub_string_from_text(text,
                                                              transform['BeginOffset'],
                                                              transform['EndOffset']-1)
            transform['Original'] = word_to_anonymize
            if transform['Type'] in ANONYMIZER_FUNCTIONS:
                transform['Anonymized'] = ANONYMIZER_FUNCTIONS[transform['Type']](word_to_anonymize)
        return transform_as_dict

    @staticmethod
    def generate_anonymous_text(text: str, transform_as_dict: dict) -> (str, dict):
        """
        Generate anonymous text
        :param text: the original text
        :param transform_as_dict: the transforms, maintained in a dictionary
        :return anonymous_text: the anonymous text
        :return transform_as_dict: the updated transform dict - for reverse transform
        """
        anonymous_text = ""
        offset = 0
        for transform in transform_as_dict['Transforms']:
            anonymous_text += text[offset:transform['BeginOffset']]
            transform['AnonBeginOffset'] = len(anonymous_text)
            anonymous_text += transform['Anonymized']
            transform['AnonEndOffset'] = len(anonymous_text)-1
            offset = transform['EndOffset']

        anonymous_text += text[offset:len(text)]

        return anonymous_text, transform_as_dict

    @staticmethod
    def generate_original_text(text: str, transform_as_dict: dict) -> str:
        """
        Generate original from anonymous
        :param text: the anonymous text
        :param transform_as_dict: the transforms, maintained in a dictionary
        :return original_text: the original text
        """
        original_text = ""
        offset = 0
        for transform in transform_as_dict['Transforms']:
            original_text += text[offset:transform['AnonBeginOffset']]
            original_text += transform['Original']
            offset = transform['AnonEndOffset'] + 1

        original_text += text[offset:len(text)]

        return original_text


ANONYMIZER_FUNCTIONS: Final[dict] = {
    PiiAnonymizer.EMAIL_KEY: PiiAnonymizer.anonymize_email,
    PiiAnonymizer.NAME_KEY: PiiAnonymizer.anonymize_name,
    PiiAnonymizer.PHONE_KEY: PiiAnonymizer.anonymize_phone_number}