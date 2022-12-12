import json
import anonymizer.anonymizer as anonymizer

VERBOSE = True


def lambda_handler(event, context):
    """
    Anonymize data in records received from client application in event object
    Output the transformed records to the destination specified by the client app (client or s3)
    :param event: A lambda event with elements included for parameters (metadata->control) and records
    :param context: A lambda context
    :return: status of processing, and, if specified in the request, the transformed records
    """

    # Access and validate the control parameters embedded in the event (event->metadata->controls)
    language_code, table_name, field_name, destination, warnings = anonymizer.get_client_control_args(event)

    # Get the records from the input event
    input_records, record_warnings = anonymizer.get_records_from_event(event)

    # Anonymize the records
    anonymized_records = anonymizer.anonymize_records(records_to_process=input_records,
                                                      text_field_name=field_name, language_code=language_code)

    # Prepare the output for the client app and write records to s3 if appropriate
    result_to_client = anonymizer.output_results(anonymized_records, destination,
                                                 anonymizer.ANONYMIZER_MODE, table_name)

    if VERBOSE:
        print("Received event: " + json.dumps(event, indent=2))

        print(f"language_code: {language_code}, table_name: {table_name}, field_name: {field_name}, "
              f"destination: {destination}, warnings: {warnings}")
        if record_warnings is not None:
            print(f"warnings: {record_warnings}")
        else:
            print(f"There were {len(input_records)} records to process")

    return result_to_client
