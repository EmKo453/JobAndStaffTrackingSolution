import zmq
import json
import threading
from current_state.models import JobState, Location
from tracking_events.models import TrackingEvent
from datetime import datetime

context = zmq.Context()

class Msg:
    def __init__(self,msg_dict):
        self.job_id = msg_dict['job_id']
        self.location = Location.objects.get(name=msg_dict['location'])
        #if not exists:
        #    raise ValueError("unknown location: {}".format(self.location))
        self.event_type = msg_dict.get('mode','I')
        self.timestamp = msg_dict['timestamp']
        
    def __str__(self):
        return f"{super().__str__()}:{self.job_id},{self.location.name},{self.event_type},{self.timestamp}"

class StateModel:
    def __init__(self,zmq_config):
        self.subsocket = context.socket(zmq.SUB)
        self.subsocket.connect(zmq_config['pub_ep'])
        self.subsocket.setsockopt(zmq.SUBSCRIBE, zmq_config['inbound_topic'].encode())

        self.pushsocket = context.socket(zmq.PUSH)
        self.pushsocket.connect(zmq_config['sub_ep'])


    def start(self):
        t = threading.Thread(target = self.run)
        t.start()

    def run(self):
        while True:
            msg = self.subsocket.recv_multipart()
            print("got ",msg)
            try:
                topic = msg[-2].decode().split('/')
                json_msg = json.loads(msg[-1])
                if topic[-1] == "jobs":
                    self.handle_scan(json_msg)
                elif topic[-1] == "custom_entry_update":
                    self.handle_custom_field_update(json_msg)
            except Exception as e:
                print("ERROR")
                print(e.msg)

    def handle_custom_field_update(self,msg):
        print(msg)
        try:
            try:
                job = JobState.objects.get(id=msg['id'])
                if 'user1' in msg.keys():
                    job.user1 = msg['user1']
                if 'user2' in msg.keys():
                    job.user2 = msg['user2']
                if 'user3' in msg.keys():
                    job.user3 = msg['user3']
                print(job)
                job.save()
            except JobState.DoesNotExist:
                print(f"Job not found with id {msg['id']}, could not update custom fields")
            #send update event
            update_msg = {
                    'id':job.id,
                    'state':'changed',
                    'location':job.location.name,
                    'timestamp':job.timestamp.isoformat() if isinstance(job.timestamp,datetime) else job.timestamp,
                    'user1':job.user1,
                    'user2':job.user2,
                    'user3':job.user3
                    }
            print(update_msg)
            #send update
            self.pushsocket.send_multipart(["state/update/changed".encode(),json.dumps(update_msg).encode()])

 
        except Exception as e:
            print("ERROR")
            print(e.msg)
    
    def handle_scan(self,raw_msg):
        print(raw_msg)
        #listen for incoming events
        try:
            #validate
            msg = Msg(raw_msg)

            #log event
            te = TrackingEvent.objects.create(job_id=msg.job_id,location=msg.location.name,event_type=msg.event_type,timestamp=msg.timestamp) 
            
            print(msg)
            
            old_location = None
            #determine new state   
            if msg.job_id[0] != 'E':
                try:
                    job = JobState.objects.get(id=msg.job_id)
                    if job.location.name == msg.location.name:
                        print("Job already scanned to location at {0}, ignoring new scan at {1}".format(job.timestamp,msg.timestamp))
                    else:
                        old_location = job.location.name
                        job.location = msg.location
                        job.timestamp = msg.timestamp
                except JobState.DoesNotExist:
                    job = JobState(id=msg.job_id,location=msg.location,timestamp=msg.timestamp)
            print(job)
            job.save()
            
            #send update event
            update_msg = {
                    'id':job.id,
                    'state':'entered',
                    'location':job.location.name,
                    'timestamp':job.timestamp.isoformat() if isinstance(job.timestamp,datetime) else job.timestamp,
                    }
            print(update_msg)
            #send update
            self.pushsocket.send_multipart(["state/update/entered".encode(),json.dumps(update_msg).encode()])

            if old_location:
                exit_msg = {
                        'id':job.id,
                        'state':'exited',
                        'location':old_location
                        }
                print(exit_msg)
                self.pushsocket.send_multipart(["state/update/exited".encode(),json.dumps(exit_msg).encode()])

        except Exception as e:
            print("ERROR")
            print (e)
