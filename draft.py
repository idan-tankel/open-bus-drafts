
import stride
from stride import api_proxy
import datetime
radius_factor = 1  # default to 1
EPSILON_lon = 0.015060*radius_factor  # 0.015060 is 1.5 km
EPSILON_lat = 0.008983*radius_factor  # 0.008983 is 1 km
TOP_FORWARD_STATIONS = 10000
kwargs = {
    "line_short_name": "47",
    "agency": "מטרופולין",
    "originated_at": "רעננה",
    "date": datetime.date(2023, 2, 21),
    "start_hour": 20,
    "end_hour": 21,
    # "line_refs": "7700"
    "gtfs_route_mkt": "19047",
    # filter by specific gtfs_route_ids
    "gtfs_route_id": 2291584
}
# iterate get


def get_gtfs_ride_stop_query_params():
    line_short_name = "47"
    agency = "מטרופולין"
    originated_at = "רעננה"
    date = datetime.date(2023, 2, 21)
    start_hour, end_hour = 20, 21
    gtfs_route_mkt = "19047"
    gtfs_route_id = 2291584
    recorded_at_time_from = datetime.datetime.combine(
        date, datetime.time(start_hour), datetime.timezone.utc)
    recorded_at_time_to = datetime.datetime.combine(
        date, datetime.time(end_hour, 59, 59), datetime.timezone.utc)
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
        "gtfs_route__route_mkt": gtfs_route_mkt,
        "gtfs_ride__gtfs_route_id": gtfs_route_id,
        # "gtfs_route__route_direction": "string",
        # "gtfs_route__route_alternative": "string",
        "gtfs_route__agency_name": agency,
        # "gtfs_route__route_type": "string"
        "order_by": "stop_sequence"
    }


def get_gtfs_ride_query_params(line_short_name, agency, originated_at, date, start_hour, end_hour, line_refs=None, gtfs_route_mkt=None, gtfs_route_id=None):
    return {
        "start_time_from": datetime.datetime.combine(date, datetime.time(start_hour), datetime.timezone.utc),
        "start_time_to": datetime.datetime.combine(date, datetime.time(end_hour, 59, 59), datetime.timezone.utc),
        "gtfs_stop__city": originated_at,
        "gtfs_route__route_short_name": line_short_name,
        "gtfs_route__agency_name": agency,
        "gtfs_route__line_refs": line_refs,
        "gtfs_route__route_mkt": gtfs_route_mkt,
        "order_by": "start_time",
        "gtfs_route_id": gtfs_route_id
    }


close_gtfs_rides = list(stride.iterate(
    '/gtfs_rides/list', params=get_gtfs_ride_query_params(**kwargs), limit=TOP_FORWARD_STATIONS))

close_gtfs_rides_hours = list(
    set([ride["start_time"] for ride in close_gtfs_rides]))
close_gtfs_rides_line_ref = list(
    set([ride["start_time"] for ride in close_gtfs_rides]))
close_gtfs_rides_operator_ref = list(
    set([ride["start_time"] for ride in close_gtfs_rides]))

gtfs_ride_stops = list(stride.iterate('/gtfs_ride_stops/list',
                       params=get_gtfs_ride_stop_query_params(), limit=TOP_FORWARD_STATIONS))
lon_lat_lists = [{"lon": stop["gtfs_stop__lon"],
                  "lat": stop["gtfs_stop__lat"]} for stop in gtfs_ride_stops]
# locations for the start and the last
# locate all the siri records by those locations


def get_siri_query_params(line_refs,operator_refs):
    date = datetime.date(2023, 2, 20)
    start_hour, end_hour = 10, 23
    recorded_at_time_from = datetime.datetime.combine(
        date, datetime.time(start_hour), datetime.timezone.utc)
    recorded_at_time_to = datetime.datetime.combine(
        date, datetime.time(end_hour, 59, 59), datetime.timezone.utc)
    return {
        "recorded_at_time_from": recorded_at_time_from,
        "recorded_at_time_to": recorded_at_time_to,
        "siri_routes__ids": 
        "lon__greater_or_equal": lon_lat_lists[0]["lon"] - EPSILON_lon,
        "lon__lower_or_equal": lon_lat_lists[0]["lon"] + EPSILON_lon,
        "lat__greater_or_equal": lon_lat_lists[0]["lat"] - EPSILON_lat,
        "lat__lower_or_equal": lon_lat_lists[0]["lat"] + EPSILON_lat,
        "order_by": "recorded_at_time"
    }


def get_siri_query_params_out():
    date = datetime.date(2023, 2, 20)
    start_hour, end_hour = 10, 23
    recorded_at_time_from = datetime.datetime.combine(
        date, datetime.time(start_hour), datetime.timezone.utc)
    recorded_at_time_to = datetime.datetime.combine(
        date, datetime.time(end_hour, 59, 59), datetime.timezone.utc)
    down_params = {
        "recorded_at_time_from": recorded_at_time_from,
        "recorded_at_time_to": recorded_at_time_to,
        "lat__lower_or_equal": lon_lat_lists[0]["lat"] - EPSILON_lat,
        "order_by": "recorded_at_time"
    }
    up_params = {
        "recorded_at_time_from": recorded_at_time_from,
        "recorded_at_time_to": recorded_at_time_to,
        "lat__greater_or_equal": lon_lat_lists[0]["lat"] + EPSILON_lat,
        "order_by": "recorded_at_time"
    }
    left_params = {
        "recorded_at_time_from": recorded_at_time_from,
        "recorded_at_time_to": recorded_at_time_to,
        "lon__greater_or_equal": lon_lat_lists[0]["lon"] + EPSILON_lon,
        "lon__lower_or_equal": lon_lat_lists[0]["lon"] + EPSILON_lon,
        "order_by": "recorded_at_time"
    }
    right_params = {
        "recorded_at_time_from": recorded_at_time_from,
        "recorded_at_time_to": recorded_at_time_to,
        "lon__greater_or_equal": lon_lat_lists[0]["lon"] - EPSILON_lon,
        "lon__lower_or_equal": lon_lat_lists[0]["lon"] + EPSILON_lon,
        "lat__greater_or_equal": lon_lat_lists[0]["lat"] - EPSILON_lat,
        "lat__lower_or_equal": lon_lat_lists[0]["lat"] + EPSILON_lat,
        "order_by": "recorded_at_time"
    }
    return right_params, left_params, up_params, down_params


def main():
    siri_records = stride.iterate('/siri_vehicle_locations/list',
                                  params=get_siri_query_params(line_refs=close_gtfs_rides_line_ref,operator_refs=close_gtfs_rides_operator_ref), limit=TOP_FORWARD_STATIONS)
    # print(list(siri_records))
    records_list = list(siri_records)
    locations_list = [(record["lat"], record["lon"], record["recorded_at_time"].strftime(
        "%H:%M:%S")) for record in records_list]
    print(locations_list)

    args = get_siri_query_params_out()
    generator_list = []
    for params in args:
        generator_list.append(stride.iterate('/siri_vehicle_locations/list',
                                               params=params, limit=TOP_FORWARD_STATIONS))
    

    # convert generators to something real
    results = [list(result) for result in results]
  # print(list(close_gtfs_rides))


if __name__ == '__main__':
    main()
# the sorting via the recorded_at_time is problematic
# there are some records that are listed under 2 separate snapshots with the same recorded_at_time
