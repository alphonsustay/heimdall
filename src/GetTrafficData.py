import requests
import datetime
import pandas as pd


def datetime_range_gen(start_, end_, freq_):
    date_time_range_ = pd.date_range(start_, end_, freq = freq_)
    return date_time_range_

def datetime_formatting(date_time_):
    return(date_time_.strftime("%Y-%m-%dT%H%%3A%M%%3A%S"))

def TrafficDataAPICall(date_time_):
    response = requests.get('https://api.data.gov.sg/v1/transport/traffic-images?date_time=' + date_time_)
    return response.json()

def get_TrafficCameraData(date_time_):
    ##### Processing traffic data obtained from API call and keeping the required data #####
    _traffic_data = TrafficDataAPICall(date_time_)
    _dt_cameras_data = extract_CamsData(_traffic_data)
    return _dt_cameras_data

def data_df_converter(dt_cam_data_list_):

    consolidated_camera_data = []
    for cams_data_list_ in dt_cam_data_list_:
        for data_ in cams_data_list_:
            data_ = flattenData(data_)
            consolidated_camera_data.append(data_)
    return pd.DataFrame(consolidated_camera_data)

def extract_CamsData(_data):
    _cam_data = _data["items"][0]["cameras"] ## json info encapsulated as dict{list[dict{timestamp, cameras[]}]}
    return _cam_data

def flattenData(data):
    flatten_dict = {
        'timestamp': data['timestamp'],
        'image': data['image'],
        'camera_id': data['camera_id'],
        'location.latitude': data['location']['latitude'],
        'location.longitude': data['location']['longitude'],
        'image_metadata.height': data['image_metadata']['height'],
        'image_metadata.width': data['image_metadata']['width'],
        'image_metadata.md5': data['image_metadata']['md5']
    }
    return flatten_dict

def get_SpecificCamData(id_, df_):
    return df_[df_['camera_id'] == id_]

def sortbytimestamp(df_):
    df_ = df_.sort_values(["timestamp"], ascending=True)
    return df_

# https://towardsdatascience.com/how-to-export-pandas-dataframe-to-csv-2038e43d9c03
def csv_exporter(df_, camera_id_):
    csv_filename = camera_id_ + ".csv"
    df_.to_csv(csv_filename, encoding='utf-8', index=False)
    print("data has been saved as %s" % (csv_filename))
    return