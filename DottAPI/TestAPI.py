import unittest

from API import app, get_last_deployment, get_rides, get_deployments
import json
import time

class APITest(unittest.TestCase):
    def setUp(self):
        self.startTime = time.time()

    def tearDown(self):
        t = time.time() - self.startTime
        print('%s: %.3f' % (self.id(), t))

    def test_get_last_deployment_vehicle(self):
        tester = app.test_client(self)
        result = get_last_deployment("vehicle_id=?")
        expect =  """SELECT deployment_id, total_rides FROM cycles WHERE vehicle_id=? ORDER BY time_cycle_started DESC LIMIT 1;"""
        self.assertEqual(result, expect)

    def test_get_last_deployment_qr_code(self):
        tester = app.test_client(self)
        result = get_last_deployment("qr_code=?")
        expect = """SELECT deployment_id, total_rides FROM cycles WHERE qr_code=? ORDER BY time_cycle_started DESC LIMIT 1;"""
        self.assertEqual(result, expect)

    def test_get_last_deployment_vehicle_qr_code(self):
        tester = app.test_client(self)
        result = get_last_deployment("vehicle_id=? AND qr_code=?")
        expect = """SELECT deployment_id, total_rides FROM cycles WHERE vehicle_id=? AND qr_code=? ORDER BY time_cycle_started DESC LIMIT 1;"""
        self.assertEqual(result, expect)

    def test_get_rides(self):
        tester = app.test_client(self)
        result = get_rides("filter")
        expect = """SELECT ride_id,time_ride_started,distance,gross_amount,start_latitude,end_latitude,start_longitude,end_longitude FROM rides WHERE deployment_id = 'filter' ORDER BY time_ride_started DESC LIMIT 5;"""
        self.assertEqual(result, expect)

    def test_get_deployments(self):
        tester = app.test_client(self)
        result = get_deployments("vehicle_id=? AND qr_code=?")
        expect = """SELECT deployment_id,time_deployment_created,time_cycle_started,total_distance,total_amount from cycles WHERE vehicle_id=? AND qr_code=? ORDER BY time_cycle_started DESC LIMIT ?;"""
        self.assertEqual(result, expect)

    def test_home(self):
        tester = app.test_client(self)
        response = tester.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b'<h1>Dott rides and deployment cycles API</h1>\n<p>API for retreive data f'
                                        b'rom rides and cycles.</p>')

    def test_page_not_found(self):
        tester = app.test_client(self)
        response = tester.get('/error_page')
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.data, b'<h1>404</h1><p>The resource could not be found.</p>')

    def test_api_filter_5_or_more_rides(self):
        tester = app.test_client(self)
        response = tester.get('/vehicles/?vehicle=zwbbrs2JbEugHojp5Y0F')
        self.assertEqual(response.status_code, 200)

        expected = [
        {
            "distance": "1272.005048155702",
            "end_latitude": "48.85520553588867",
            "end_longitude": "2.265850067138672",
            "gross_amount": "1.85",
            "ride_id": "iVIU2x5sookKeK0Vfc0U",
            "start_latitude": "48.865447998046875",
            "start_longitude": "2.2735934257507324",
            "time_ride_started": "2019-05-14T18:34:41.136+01:00"
        },
        {
            "distance": "1.2936342956951168",
            "end_latitude": "48.86540985107422",
            "end_longitude": "2.2738165855407715",
            "gross_amount": "1.02",
            "ride_id": "WAt1CAdl1IGoiT4UKDSL",
            "start_latitude": "48.86540222167969",
            "start_longitude": "2.273803234100342",
            "time_ride_started": "2019-05-14T08:58:18.974+01:00"
        },
        {
            "distance": "5.361161773541194",
            "end_latitude": "48.86541366577149",
            "end_longitude": "2.2737832069396973",
            "gross_amount": "1.18",
            "ride_id": "HFGegQWzqcwi3tTIEbAn",
            "start_latitude": "48.86545181274414",
            "start_longitude": "2.273738384246826",
            "time_ride_started": "2019-05-14T08:55:52.330+01:00"
        },
        {
            "distance": "1274.0852672178503",
            "end_latitude": "48.866180419921875",
            "end_longitude": "2.2741332054138184",
            "gross_amount": "1.97",
            "ride_id": "hdkNS4ZvxucMH2RLxtrT",
            "start_latitude": "48.85667037963867",
            "start_longitude": "2.264418363571167",
            "time_ride_started": "2019-05-13T09:55:35.194+01:00"
        },
        {
            "distance": "482.13076197007547",
            "end_latitude": "48.85655212402344",
            "end_longitude": "2.264169931411743",
            "gross_amount": "1.93",
            "ride_id": "0pQmQKenceS7htjSkIjx",
            "start_latitude": "48.85563278198242",
            "start_longitude": "2.2706100940704346",
            "time_ride_started": "2019-05-13T09:30:18.994+01:00"
        }
    ]
        self.assertEqual(json.loads(response.data), expected)

    def test_api_filter_1_to_4_rides(self):
        tester = app.test_client(self)
        response = tester.get('/vehicles/?vehicle=020B80o2utOLyVylwWiT')
        self.assertEqual(response.status_code, 200)
        expected = [
            {
                "distance": "248.11483142199705",
                "end_latitude": "48.87224",
                "end_longitude": "2.301074",
                "gross_amount": "4.1",
                "ride_id": "dnirnhc0P2Fp9HWInOWy",
                "start_latitude": "48.87001",
                "start_longitude": "2.300956",
                "time_ride_started": "2019-05-30T23:52:49.459+01:00"
            },
            {
                "distance": "1852.8251975161577",
                "end_latitude": "48.870132",
                "end_longitude": "2.300919",
                "gross_amount": "5.64",
                "ride_id": "TE1kScdDXmM0L3kW5HFc",
                "start_latitude": "48.871028",
                "start_longitude": "2.326215",
                "time_ride_started": "2019-05-30T23:10:24.804+01:00"
            },
            {
                "distance": "476.0806970357944",
                "end_latitude": "48.871015",
                "end_longitude": "2.326248",
                "gross_amount": "2.87",
                "ride_id": "SuN3W6b60BOHJMQf5IsI",
                "start_latitude": "48.870646",
                "start_longitude": "2.332733",
                "time_ride_started": "2019-05-29T22:37:20.363+01:00"
            },
            {
                "distance": "494.5561808192783",
                "end_latitude": "48.870637",
                "end_longitude": "2.332781",
                "gross_amount": "2.1",
                "ride_id": "5yAXYCyk827zG6MEGDtx",
                "start_latitude": "48.872131",
                "start_longitude": "2.33915",
                "time_ride_started": "2019-05-29T22:28:52.392+01:00"
            },
            {
                "deployment_id": "xnV7knQkzRLKElsTQM98",
                "time_cycle_started": "2019-05-29T19:28:50.743+01:00",
                "time_deployment_created": "2019-05-28T21:37:48.830+01:00",
                "total_amount": "14.709999999999999",
                "total_distance": "3071.576906793228"
            }
        ]
        self.assertEqual(json.loads(response.data), expected)

    def test_api_filter_no_rides(self):
        tester = app.test_client(self)
        response = tester.get('/vehicles/?vehicle=Ojnoprd6NJmiAT0DwsM9')
        self.assertEqual(response.status_code, 200)
        expected = [
            {
                "deployment_id": "RMSUT3TOjm084LrlgIOp",
                "time_cycle_started": "2019-05-04T15:01:56.033+01:00",
                "time_deployment_created": "2019-05-04T15:01:08.781+01:00",
                "total_amount": "",
                "total_distance": ""
            },
            {
                "deployment_id": "rMt9Dv75xINqsbS5neh8",
                "time_cycle_started": "2019-05-04T14:59:34.656+01:00",
                "time_deployment_created": "2019-05-04T14:40:26.811+01:00",
                "total_amount": "",
                "total_distance": ""
            },
            {
                "deployment_id": "PtZsZB6aqJYF9Ss99o9r",
                "time_cycle_started": "2019-05-04T14:39:55.229+01:00",
                "time_deployment_created": "2019-05-04T14:26:59.987+01:00",
                "total_amount": "",
                "total_distance": ""
            },
            {
                "deployment_id": "3cJckycvrBgLFVUbRMaT",
                "time_cycle_started": "2019-05-04T14:26:40.054+01:00",
                "time_deployment_created": "2019-05-04T14:01:49.103+01:00",
                "total_amount": "",
                "total_distance": ""
            },
            {
                "deployment_id": "riE3gp2FpnfTcKGowkiF",
                "time_cycle_started": "2019-05-04T13:58:42.037+01:00",
                "time_deployment_created": "2019-05-04T13:49:13.110+01:00",
                "total_amount": "",
                "total_distance": ""
            }
        ]
        self.assertEqual(json.loads(response.data), expected)

    def test_api_filter_no_deployments(self):
        tester = app.test_client(self)
        response = tester.get('/vehicles/?vehicle=020B80o2utOLyVylwWiT1')
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.data, b'<h1>404</h1><p>The resource could not be found.</p>')

    def test_api_filter_qr_code(self):
        tester = app.test_client(self)
        response = tester.get('/vehicles/?qrcode=Z1CRVF')
        self.assertEqual(response.status_code, 200)
        expected = [
            {
                "deployment_id": "B4zTz7zHSOQ6NOpcd2kZ",
                "time_cycle_started": "2019-05-04T20:57:20.662+01:00",
                "time_deployment_created": "2019-05-03T19:46:26.638+01:00",
                "total_amount": "",
                "total_distance": ""
            },
            {
                "deployment_id": "LyPw96hdjcRjdZpDCc4C",
                "time_cycle_started": "2019-05-03T17:57:19.594+01:00",
                "time_deployment_created": "2019-05-03T12:44:34.713+01:00",
                "total_amount": "",
                "total_distance": ""
            }
        ]
        self.assertEqual(json.loads(response.data), expected)

    def test_api_filter_qr_code_vehicle(self):
        tester = app.test_client(self)
        response = tester.get('/vehicles/?qrcode=00SVYC&vehicle=zlgO7gTd13bjDC0eTSMM')
        self.assertEqual(response.status_code, 200)
        expected = [
            {
                "deployment_id": "H9FpC32Zblbu60d1fW0G",
                "time_cycle_started": "2019-05-06T06:23:34.499+01:00",
                "time_deployment_created": "2019-04-15T11:29:42.883+01:00",
                "total_amount": "",
                "total_distance": ""
            }
        ]
        self.assertEqual(json.loads(response.data), expected)

    def test_api_filter_perf_1_to4(self):
        tester = app.test_client(self)
        a = 0
        for i in range(5001):
            response = tester.get('/vehicles/?vehicle=020B80o2utOLyVylwWiT')
            if response == 200:
                a = i
        self.assertEqual(i, 5000)

    def test_api_filter_perf_no_rides(self):
        tester = app.test_client(self)
        a = 0
        for i in range(5001):
            response = tester.get('/vehicles/?vehicle=Ojnoprd6NJmiAT0DwsM9')
            if response == 200:
                a = i
        self.assertEqual(i, 5000)

    def test_api_filter_perf_5_or_more(self):
        tester = app.test_client(self)
        a = 0
        for i in range(5001):
            response = tester.get('/vehicles/?vehicle=zwbbrs2JbEugHojp5Y0F')
            if response == 200:
                a = i
        self.assertEqual(i, 5000)





if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(APITest)
    unittest.TextTestRunner(verbosity=0).run(suite)
