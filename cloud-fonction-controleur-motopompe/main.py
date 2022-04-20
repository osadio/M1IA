import base64
from google.cloud import bigquery
from google.cloud import iot_v1

def water_pump_command(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # Parameters
    project_id = event['attributes']['projectId']
    cloud_region = event['attributes']['deviceRegistryLocation']
    registry_id = event['attributes']['deviceRegistryId']
    device_id = event['attributes']['deviceId']

    # Get water level sensor data (EMPTY, SEMI_FULL or FULL)
    water_level = base64.b64decode(event['data']).decode('utf-8')
    print(f"Receiving water sensor data : {water_level}")
    
    # Predict based on the last 12 hours recorded sensors data (temperature, humidity, month)
    bq_client = bigquery.Client()
    query = """
     SELECT
       *
     FROM
       ML.PREDICT(MODEL `ml_models.rainfall_prediction`, (
     SELECT
       CAST(humidity as INT64) AS humidity,
       CAST(temperature as FLOAT64) AS temperature,
       EXTRACT(MONTH FROM timestamp) as month
     FROM
        `dataset_pipeline_ousmane.temperature_humidite_avg`
     WHERE timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) AND CURRENT_TIMESTAMP() 
     ))
     ORDER BY predicted_label DESC
     LIMIT 1
    """
  
    query_job = bq_client.query(query)

    will_rain = False
    for row in query_job:
        # Row((1, [{'label': 1, 'prob': 0.5441665957315596}, {'label': 0, 'prob': 0.45583340426844043}], 87, 23.9, 4), {'predicted_label': 0, 'predicted_label_probs': 1, 'humidity': 2, 'temperature': 3, 'month': 4})
        if row[0] == 1:
            will_rain = True
    print(f"Rain prediction : {will_rain}")

    # Give prescription (NOT_FILL, HALF_FILL or COMPLETELY_FILL) based on rules engine
    filling_level = "NOT_FILL"
    if water_level == "EMPTY":
        if will_rain :
            filling_level = "HALF_FILL"
        else:
            filling_level = "COMPLETELY_FILL"
    elif water_level == "SEMI_FULL":
        if will_rain :
            filling_level = "NOT_FILL"
        else:
            filling_level = "COMPLETELY_FILL"
    elif water_level == "FULL":
        filling_level = "NOT_FILL"
    print(f"Water pump command : {filling_level}")

    # Sending command
    print("Sending command to device")
    iot_client = iot_v1.DeviceManagerClient()
    device_path = iot_client.device_path(project_id, cloud_region, registry_id, device_id)

    command = filling_level
    data = command.encode("utf-8")

    return iot_client.send_command_to_device(
       request={"name": device_path, "binary_data": data}
    )
        




    

