from os import closerange
import pandas as pd
import concurrent.futures
import GetTrafficData as utils

start  = '2019/01/01'
end = '2019/02/01'
f = 'T'
CamData_response_list = []

##### Generating the list of datatime based on required frequency #####
_datetime_list = utils.datetime_range_gen(start, end, f)

# We can use a with statement to ensure threads are cleaned up promptly
with concurrent.futures.ThreadPoolExecutor(max_workers = 5) as exector:
    # Start the load operations and mark each future with its URL
    future_to_datetime = {exector.submit(utils.get_TrafficCameraData, utils.datetime_formatting(date_time)): date_time for date_time in _datetime_list}
    for future in concurrent.futures.as_completed(future_to_datetime):
        date_time = future_to_datetime[future]
        try:
            data = future.result()
        except Exception as exc:
            print('%r generated an exception: %s' % (date_time, exc))
        else:
            CamData_response_list.append(data)
            print('%r data added to response list' %(date_time))

print(len(CamData_response_list))

camData_df = utils.data_df_converter(CamData_response_list)
print(len(camData_df))
print(len(camData_df.columns))

camera_id = "1709"

print("Extracting data for camera ID: %s" %(camera_id))
specific_cam_df = utils.get_SpecificCamData(camera_id, camData_df)
specific_cam_df = specific_cam_df.drop_duplicates(subset = 'image')
print(len(specific_cam_df))
print(len(specific_cam_df.columns))

specific_cam_df = utils.sortbytimestamp(specific_cam_df)

utils.csv_exporter(specific_cam_df, camera_id, start, end)