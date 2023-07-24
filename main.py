import boto3
import json
import datetime
from datetime import timedelta
from aws_s3_secret import aws_access_key, aws_secret_key


class AwsS3Client:
    def __init__(self, access_key: str, secret_key: str):
        self.access_key = access_key
        self.secret_key = secret_key

    def get_connection(self):
        return boto3.resource(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

    def get_bucket_by_name(self, bucket_name):
        s3_connection = self.get_connection()
        return s3_connection.Bucket(bucket_name)

    def upload_object_by_bucket_name(self, bucket_name, key, value):
        s3_connection = self.get_connection()
        s3_connection.Object(bucket_name, key).put(Body=value)

    def get_s3_objects_with_filter_by_year_month(self, bucket_name, year, month):
        s3_bucket = self.get_bucket_by_name(bucket_name)
        return s3_bucket.objects.filter(Prefix=f'{year}/{month}')


class Transformer:
    def __init__(self, access_key, secret_key):
        self.aws_s3_client = AwsS3Client(
            access_key=access_key,
            secret_key=secret_key
        )

    @staticmethod
    def filter_arvlcd(arvlcd):
        if arvlcd == "1":
            return True
        return False

    @staticmethod
    def filter_updn_line(updn_line):
        if updn_line == "1":
            return True
        return False

    @staticmethod
    def mapping_subway_id(subway_id):
        subway_id_map = {
            "1001": "1호선",
            "1002": "2호선",
            "1003": "3호선",
            "1004": "4호선",
            "1005": "5호선",
            "1006": "6호선",
            "1007": "7호선",
            "1008": "8호선",
            "1009": "9호선",
            "1061": "중앙선",
            "1063": "경의중앙선",
            "1065": "공항철도",
            "1067": "경춘선",
            "1075": "수인분당선",
            "1077": "신분당선",
            "1092": "우이신설선",
        }
        return subway_id_map[subway_id]

    @staticmethod
    def get_datetime_by_string(date_time):
        return datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')

    @staticmethod
    def get_time_by_string(date_time):
        return datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S').time()

    @staticmethod
    def get_date_by_string(date_time):
        return datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S').date()

    @staticmethod
    def parse_s3_object_body_with_json(s3_object):
        s3_object_body = s3_object.get()['Body'].read().decode('utf-8')
        return json.loads(s3_object_body)

    def filter_weekday(self, recptn_dt):
        date_time_obj = self.get_datetime_by_string(recptn_dt)
        weekno = date_time_obj.weekday()
        if weekno < 5:
            return 1
        elif weekno == 5:
            return 2
        else:
            return 3

    def parse_realtime_arrival_list_by_json(self, s3_object):
        return self.parse_s3_object_body_with_json(s3_object)["realtimeArrivalList"]

    def translation_objects_with_uploads(self, year, month, target_bucket_name, destination_bucket_name):
        for s3_object in self.aws_s3_client.get_s3_objects_with_filter_by_year_month(target_bucket_name, year, month):
            s3_object_body = self.parse_realtime_arrival_list_by_json(s3_object)
            result = self.translation_object(s3_object_body)
            self.aws_s3_client.upload_object_by_bucket_name(destination_bucket_name, s3_object.key, result)

    def translation_object(self, s3_object_body):
        translation_result = ""
        for item in s3_object_body:
            arvlcd = item["arvlCd"]
            station_num = item["statnNm"]
            updn_line = item["updnLine"]
            subway_id = item["subwayId"]
            recptn_dt = item["recptnDt"]

            if self.filter_arvlcd(arvlcd):
                station_item = {"stationNm": station_num + "역", "inOutTag": 1}
                if self.filter_updn_line(updn_line):
                    station_item["inOutTag"] = 2
                station_item["lineNum"] = self.mapping_subway_id(subway_id)
                station_item["weekTag"] = self.filter_weekday(recptn_dt)
                station_item["arriveTime"] = str(self.get_time_by_string(recptn_dt))
                station_item["arriveDate"] = str(self.get_date_by_string(recptn_dt))
                station_item = json.dumps(station_item, ensure_ascii=False)
                translation_result += str(station_item) + "\n"
        return translation_result


if __name__ == "__main__":
    start_datetime = datetime.datetime.strptime("2023-06-25", '%Y-%m-%d')
    while True:
        # please fill aws access key & secret key before run
        transformer = Transformer(
            access_key=aws_access_key, #change variable to string
            secret_key=aws_secret_key #change variable to string
        )
        transformer.translation_objects_with_uploads(
            year=start_datetime.year,
            month=start_datetime.month,
            target_bucket_name="italian_bmt_bucket",
            destination_bucket_name='italian-bmt-elastic-bucket'
        )
        start_datetime = start_datetime + timedelta(days=1)
