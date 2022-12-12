import json

import anonymizer.ScrubTransforms as ScrubTransforms
import anonymizer.InferenceFacade as ComprehendFacade
import anonymizer.DynamoDBFacade as DynamoDBFacade
import anonymizer.S3Facade as S3Facade

my_dynamo = DynamoDBFacade.DynamoDBFacade(region="us-east-1")
my_s3 = S3Facade.S3Facade(bucket_name="pii-scrub-service.poc.ab3.ai")


# Output Destination Options
DESTINATION_CLIENT = 'client'
DESTINATION_S3 = 's3'

# The mode of processing
ANONYMIZER_MODE = 'ANONYMIZER'
REVERT_MODE = 'REVERT'


def output_results_to_client(records) -> dict:
    """
    Output the response to the calling client
    :param records: the processed record set, anonymized or reverted or null if the records output is
    directed to persistent storage
    :return: dict that includes status, headers, and body if records is not None
    """
    output = {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
    }
    if records is not None:
        # output['body'] = json.dumps(records, sort_keys=True, indent=4)
        # output['body'] = json.dumps(records, sort_keys=False, indent=4)
        output['body'] = json.dumps(records)
    return output


def output_results_to_s3(records, mode, table_name) -> dict:
    if mode == ANONYMIZER_MODE:
        s3_location = my_s3.write_anonymized_records(records, table_name)
    else:
        raise ValueError('Not supported yet')
    output = dict(statusCode=200, headers={
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
    })
    output['body'] = f"output saved to AWS S3: {s3_location}"
    return output


def output_results(records: list, destination: str, mode: str, table_name: str):
    """
    Output the processed records to the appropriate destination
    :param records: the processed record set, anonymized or reverted
    :param destination: the destination to output the processed records - the client app or s3
    :param mode: anonymize or revert - needed for s3 output
    :param table_name: the name of the table being processed - used for s3 output
    :return: dict that includes status, headers, and body with the records if destination is client
    """
    if destination == DESTINATION_CLIENT:
        return output_results_to_client(records)
    elif destination == DESTINATION_S3:
        return output_results_to_s3(records, mode, table_name)
    else:
        raise ValueError('Invalid output destination')


def anonymize_records(records_to_process: list, text_field_name: str, language_code='en') -> list:
    """
    :param records_to_process set of records to process
    :param text_field_name: name of the field containing the text to be anonymized
    :param language_code: language code for the text to be anonymized
    :return: list of anonymized records
    """
    my_comprehend = ComprehendFacade.InferenceFacade(language_code=language_code)
    my_scrub_xform = ScrubTransforms.ScrubTransforms(language_code=language_code)
    anonymized_records = []
    for record in records_to_process:
        text = record.pop(text_field_name, "No text provided")
        # anonymize the text
        base_transforms = my_comprehend.detect_pii_entities(text)
        anon_transforms = my_scrub_xform.anonymize_text(text, base_transforms)
        anonymized_text, complete_transform = my_scrub_xform.generate_anonymous_text(text, anon_transforms)
        # persist anon_dict to DynamoDB
        guid = my_dynamo.put_scrub_xform(complete_transform)
        # create the anonymized record
        anonymized_record = {
            text_field_name: anonymized_text,
            'revert_key': guid
        }
        # merge anonymized_record and record
        anonymized_record.update(record)
        # add the anonymized record to the records
        anonymized_records.append(anonymized_record)
    return anonymized_records


def revert_records(records_to_process: list, text_field_name: str) -> list:
    """
    Revert the anonymized records back to their original
    :param records_to_process: the records to revert
    :param text_field_name: the name of the field containing the text to be anonymized
    :return: list of anonymized records
    """
    my_pii_anon = ScrubTransforms.ScrubTransforms()
    reverted_records = []
    for record in records_to_process:
        guid = record.pop('revert_key', "No guid provided")
        anon_text = record.pop(text_field_name, "No text provided")
        # get the transform used from  DynamoDB
        saved_transform = my_dynamo.get_scrub_xform(guid)
        original_text = my_pii_anon.generate_original_text(anon_text, saved_transform)
        # create the original record
        original_record = {
            text_field_name: original_text
        }
        # merge original_record and record
        original_record.update(record)
        # add the original record to the records
        reverted_records.append(original_record)
    return reverted_records


def xxx_anonymize(records_to_process: list, text_field_name: str, language_code='en',
              target=DESTINATION_CLIENT, table_name=None) -> list:
    """
    :param records_to_process: set of records to anonymize
    :param text_field_name: name of the field containing the text to be anonymized
    :param language_code: language code for the text to be anonymized
    :param target: the target of the output, either DESTINATION_CLIENT or DESTINATION_S3
    :param table_name: used in the file key, if the output is to DESTINATION_S3
    :return: processing status and if the output target is DESTINATION_CLIENT then the records are returned as the body
    """
    anon_records = anonymize_records(records_to_process=records_to_process,
                                     text_field_name=text_field_name,
                                     language_code=language_code)
    if target == DESTINATION_S3:
        my_s3.write_anonymized_records(records=anon_records, table_name=table_name)
        anon_records = None     # set the records to return to client as null
    return anon_records


def yyy_revert(records_to_process: list, text_field_name: str, target=DESTINATION_CLIENT, table_name=None) -> list:
    """
    :param records_to_process: set of records to revert to their original
    :param text_field_name: name of the field containing the text to be reverted
    :param target: the target of the output, either DESTINATION_CLIENT or DESTINATION_S3
    :param table_name: used in the file key, if the output is to DESTINATION_S3
    :return: processing status and if the output target is DESTINATION_CLIENT then the records are returned as the body
    """
    reverted_records = revert_records(records_to_process=records_to_process,
                                      text_field_name=text_field_name)
    if target == DESTINATION_S3:
        my_s3.write_reverted_records(records=reverted_records, table_name=table_name)
        reverted_records = None     # set the records to return to client as null
    return reverted_records


def sanitize_table_name(table_name: str, default: str = 'default') -> (str, str):
    """
    :param table_name: the name of the table to sanitize
    :param default: the default name to use if the table name is not specified
    :return: the sanitized table name and the default name
    """
    if table_name is None:
        return default, None     # table_name is only used if the destination is S3, no warning if not set
    warning = None
    orig_table_name = table_name
    # sanitize to remove bad chars
    table_name = table_name.replace('-', '_')
    table_name = table_name.replace('.', '_')
    table_name = table_name.replace(' ', '_')
    table_name = table_name.translate(str.maketrans('', '', ' !@#$%^&*()+=-[]{};:,./<>?`~'))
    if orig_table_name != table_name:
        warning = f"\'table_name\' parameter \'{orig_table_name}\' contains unacceptable characters. " \
                      f"Sanitized to \'{table_name}\'"
    return table_name, warning


def sanitize_field_name(field_name: str, default: str) -> (str, str):
    """
    :param field_name: the name of the field to sanitize
    :param default: the default value to use if the field name is empty
    :return: the sanitized field name, warning message if appropriate
    """
    warning = None
    if field_name is None:
        return default, f"Using default setting for parameter \'field_name\': \'{default}\'"
    orig_field_name = field_name
    field_name = field_name.replace('-', '_')
    field_name = field_name.replace('.', '_')
    field_name = field_name.replace(' ', '_')
    # remove other non-alphanumeric characters
    field_name = field_name.translate(str.maketrans('', '', ' !@#$%^&*()+=-[]{};:,./<>?`~'))
    if orig_field_name != field_name:
        warning = f"\'field_name\' parameter \'{orig_field_name}\' contains unacceptable characters. " \
                      f"Sanitized to \'{field_name}\'"
    return field_name, warning


def sanitize_language_code(language_code: str, default: str = 'en') -> (str, str):
    """
    :param language_code: the language code to sanitize
    :param default: the default language code
    :return: the sanitized language code, warning message if appropriate
    """
    warning = None
    if language_code is None:
        return default, f"Using default setting for parameter \'language_code\': \'{default}\'"
    language_code = language_code.lower()
    if language_code not in ['en', 'fr']:
        warning = f"language_code parameter \'{language_code}\' not recognized. Sanitized to \'{language_code}\'"
        language_code = default
    return language_code, warning


def sanitize_results_destination(destination: str, default=DESTINATION_CLIENT) -> (str, str):
    """
    :param destination: the destination to sanitize
    :param default: the default destination
    :return: the sanitized destination
    """
    warning = None
    if destination is None:
        return default, f"Using default setting for parameter \'destination\': \'{default}\'"
    orig_destination = destination
    destination = destination.lower()
    if destination not in [DESTINATION_S3, DESTINATION_CLIENT]:
        destination = default
        warning = f"destination parameter \'{orig_destination}\' is not an option. Sanitized to \'{destination}\'"
    return destination, warning


def get_client_control_args(event):
    """
    :param event: the event to process
    :return: the language code, table name, field name, results destination, and warnings
    """
    warnings = []
    if event.get('metadata', {}).get('control', {}) and type(event['metadata']['control']) is dict:
        control_args = event['metadata']['control']
        language_code, warning = sanitize_language_code(control_args.get('language_code', None), 'en')
        if warning is not None:
            warnings.append(warning)
        table_name, warning = sanitize_table_name(control_args.get('table_name', None), 'default')
        if warning is not None:
            warnings.append(warning)
        field_name, warning = sanitize_field_name(control_args.get('field_name', None), 'text')
        if warning is not None:
            warnings.append(warning)
        destination, warning = sanitize_results_destination(control_args.get('destination', None), DESTINATION_CLIENT)
        if warning is not None:
            warnings.append(warning)
    else:
        language_code, table_name, field_name, destination = 'en', 'default', 'text', DESTINATION_CLIENT
        warnings.append("No control parameters in request. Using default values.")
    if len(warnings) == 0:
        warnings = None
    return language_code, table_name, field_name, destination, warnings


def get_records_from_event(event):
    """
    :param event: the event to process
    :return: the records present as a list, and any warnings
    """
    warnings = None
    records = event.get('records', None)
    if records is None or len(records) == 0:
        warnings = "No records in the input request to process"
    return records, warnings
