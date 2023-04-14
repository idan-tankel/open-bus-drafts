import stride
from stride import api_proxy
import datetime
EPSILON = 100000
TOP_FORWARD_STATIONS = 5
# iterate get
def get_gtfs_ride_stop_query_params():
    line_short_name = "47"
    agency = "מטרופולין"
    originated_at = "רעננה"
    date = datetime.date(2023,2, 21)
    start_hour, end_hour = 20,21 
    recorded_at_time_from = datetime.datetime.combine(date, datetime.time(start_hour), datetime.timezone.utc)
    recorded_at_time_to = datetime.datetime.combine(date, datetime.time(end_hour, 59, 59), datetime.timezone.utc)
    return {
        # "stop_sequence": i,
        # "pickup_type": 0,
        # "drop_off_type": 0,
        # "shape_dist_traveled": 0,
        # "gtfs_ride__journey_ref": "string",
        "gtfs_ride__start_time_from": recorded_at_time_from,
        "gtfs_ride__start_time_to": recorded_at_time_to,
        "gtfs_stop__city": originated_at,
        "gtfs_stop__date": date,
        "gtfs_route__date": date,
        "gtfs_route__route_short_name": line_short_name,
        # "gtfs_route__route_long_name": "string",
        # "gtfs_route__route_mkt": "string",
        # "gtfs_route__route_direction": "string",
        # "gtfs_route__route_alternative": "string",
        "gtfs_route__agency_name": agency,
        # "gtfs_route__route_type": "string"
        "order_by": "stop_sequence"
  }


gtfs_ride_stops = list(stride.iterate('/gtfs_ride_stops/list',params=get_gtfs_ride_stop_query_params(),limit=TOP_FORWARD_STATIONS))
print(gtfs_ride_stops)
lon_lat_lists = [{"lon": stop["gtfs_stop__lon"],"lat": stop["gtfs_stop__lat"]} for stop in gtfs_ride_stops]
# locations for the start and the last
# locate all the siri records by those locations


def get_siri_query_params():
    date = datetime.date(2023,2, 21)
    start_hour, end_hour = 20,21 
    recorded_at_time_from = datetime.datetime.combine(date, datetime.time(start_hour), datetime.timezone.utc)
    recorded_at_time_to = datetime.datetime.combine(date, datetime.time(end_hour, 59, 59), datetime.timezone.utc)
    return {
        "recorded_at_time_from": recorded_at_time_from,
        "recorded_at_time_to": recorded_at_time_to,
        "lon__greater_or_equal": lon_lat_lists[0]["lat"]+EPSILON,
        "lon__lower_or_equal": lon_lat_lists[0]["lat"] - EPSILON,
        "lat__greater_or_equal": lon_lat_lists[0]["lon"]+EPSILON,
        "lat__lower_or_equal": lon_lat_lists[0]["lon"] - EPSILON,
        "order_by": "recorded_at_time"
  }


siri_records = list(stride.iterate('/siri_vehicle_locations/list',params=get_siri_query_params(),limit=TOP_FORWARD_STATIONS))
print(siri_records)
