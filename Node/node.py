import socket
import os
import json
import threading
import time
import random
import math


class Node:
    def __init__(self) -> None:
        self.name = os.getenv("NAME")
        self.node_list = json.loads(os.getenv("NODELIST"))
        self.node_list.remove(self.name)
        self.timeout = 0.1 * random.uniform(2.5, 3.5)
        self.alive = True
        self.send = False
        self.curr_leader = None
        self.curr_term = 0
        self.curr_term_votes = 0
        self.logs = []
        self.curr_index = -1
        self.prev_index = None
        self.commit_index = -1
        self.highest_index = None
        self.replicate_count = 0
        self.state = "FOLLOWER"
        self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.socket.bind((self.name, 5555))

    def generateMessage(self, request):
        msg = {
            "sender_name": self.name,
            "term": self.curr_term,
            "request": request,
            "key": None,
            "value": None,
        }
        if request == "LEADER_INFO":
            msg["key"] = "LEADER"
            msg["value"] = self.curr_leader
        elif request == "COMMITTED_LOGS":
            msg["request"] = "RETRIEVE"
            msg["key"] = "COMMITTED_LOGS"
            msg["value"] = json.dumps(self.log)
        elif request == "F":
            msg["key"] = "success"
            msg["value"] = "False"
            msg["request"] = "LOG_ACK"
        elif request == "T":
            msg["key"] = "success"
            msg["value"] = "True"
            msg["request"] = "LOG_ACK"
        return json.dumps(msg).encode()

    def main(self):
        while self.alive:
            curtime = time.time()
            endtime = time.time() + self.timeout
            heartbeat = False
            while curtime < endtime:
                self.socket.settimeout(0)
                try:
                    msg = self.socket.recv(1024)
                    msg = json.loads(msg.decode())

                    request = msg["request"]
                    if request == "APPEND_RPC":
                        heartbeat = True
                        self.appendRpc(msg)
                    elif request == "VOTE_REQUEST":
                        self.sendVote(msg)
                    elif request == "VOTE_ACK":
                        self.handleVotes()
                    elif request == "LOG_ACK" and msg["key"]:
                        self.replicate_count += 1
                    elif request == "LOG_ACK" and not msg["key"]:
                        self.handleLogConflict(msg)
                    elif request == "COMMIT_LOG":
                        self.commitLog()
                except BlockingIOError:
                    pass
                curtime = time.time()

            if self.state == "FOLLOWER":
                if not heartbeat:
                    print(self.name + " IS BECOMING CANDIDATE")
                    self.state = "CANDIDATE"
            elif self.state == "CANDIDATE":
                # start a new thread to avoid IO blocking
                threading.Thread(target=self.requestVote).start()
            self.timeout = 0.1 * random.uniform(2.5, 3.5)

    def appendRpc(self, msg):
        if self.state == "CANDIDATE":
            self.convertToFollower()
        elif self.state == "FOLLOWER":
            if msg["term"] < self.curr_term:
                return
            self.curr_leader = msg["sender_name"]
            self.curr_term = int(msg["term"])
            if "prev_index" in msg:
                self.replicateLog(msg)

    def replicateLog(self, msg):
        # only replicate if curr index is not up to date
        if msg["prev_index"]:
            if self.curr_index < msg["prev_index"]:
                prev_index = msg["prev_index"]
                response = None
                if prev_index < len(self.logs):
                    log = self.logs[self.prev_index]
                    if log["term"] == msg["prev_term"] and log["index"] == prev_index:
                        for entry in msg["new_entries"]:
                            self.logs.append(entry)
                            self.curr_index += 1
                            self.highest_index = entry["index"]
                        response = self.generateMessage("T")
                    else:
                        response = json.loads(self.generateMessage("F").decode())
                        response["last_entry"] = self.getNextIndex(msg)
                else:
                    response = json.loads(self.generateMessage("F").decode())
                    response["last_entry"] = self.getNextIndex(msg)

                response = json.dumps(response).encode()
                self.socket.sendto(response, (msg["sender_name"], 5555))
            else:
                if self.logs[msg["prev_index"]]["term"] == msg["prev_term"]:
                    while self.curr_index != msg["prev_index"]:
                        self.logs.pop()

                    for entry in msg["new_entries"]:
                        self.logs.append(entry)
                        self.curr_index += 1

                    self.socket.sendto(
                        self.generateMessage("T"), (msg["sender_name"], 5555)
                    )
                else:
                    response = json.loads(self.generateMessage("F").decode())
                    response = self.getNextIndex(msg)
                    response = json.dumps(response).encode()
        else:
            if self.curr_index < 0:
                self.curr_index = 0
                self.logs.append(msg["new_entries"][0])
                self.highest_index = msg["new_entries"][0]["index"]
                print(self.name + " REPLICATED FIRST LOG SUCCESSFULLY")
                print(self.logs)
                self.socket.sendto(
                    self.generateMessage("T"), (msg["sender_name"], 5555)
                )

    def getNextIndex(self, msg):
        term = (
            msg["last_entry"]["term"] - 1
            if "next_index" in msg
            else self.logs[-1]["term"]
        )
        print(term, "LOOKING FOR NEXT INDEX WITH TERM")
        entry = None
        for log in self.logs:
            if log["term"] == term:
                entry = log
                print(entry + " SENDING LAST ENTRY")
        return entry

    def handleLogConflict(self, msg):
        sender = msg["sender_name"]
        last_entry = msg["last_entry"]

        msg = self.generateMessage("APPEND_RPC")
        msg = json.loads(msg.decode())
        msg["prev_index"] = last_entry["index"] - 1
        msg["prev_term"] = self.logs[last_entry["index"] - 1]["term"]
        msg["new_logs"] = self.logs[last_entry["index"] :]
        msg = json.dumps(msg).encode()
        self.socket.sendto(msg, (sender, 5555))

    # send vote request
    def requestVote(self):
        print(self.name, " IS REQUESTNG A VOTE")
        self.curr_term += 1
        for node in self.node_list:
            msg = self.generateMessage("VOTE_REQUEST")
            self.socket.sendto(msg, (node, 5555))

    def sendVote(self, msg):
        print(self.name + " IS SENDIND VOTE TO" + msg["sender_name"])
        if not self.state == "LEADER":
            if msg["term"] > self.curr_term:
                self.curr_term = msg["term"]
                self.voted_to = msg["sender_name"]
                m = self.generateMessage("VOTE_ACK")
                self.socket.sendto(m, (msg["sender_name"], 5555))

    def handleVotes(self):
        print(self.name, " IS COUNTING VOTES")
        if self.state == "CANDIDATE":
            self.curr_term_votes += 1
            if self.curr_term_votes > math.floor(len(self.node_list) / 2) + 1:
                self.convertToLeader()

    def convertToLeader(self):
        self.curr_leader = self.name
        print("CURRENT LEADER IS: ", self.name)
        self.curr_term_votes = 0
        self.state = "LEADER"
        self.send = True
        heartbeat = threading.Thread(target=self.sendHeartbeat)
        heartbeat.start()

    def convertToFollower(self):
        print(self.name + " CONVERTING BACK TO FOLLOWER")
        self.send = False
        self.curr_term_votes = 0
        self.replicate_count = 0
        self.state = "FOLLOWER"

    def sendHeartbeat(self):
        self.logs.append({"index": 0, "value": 1, "term": self.curr_term})
        self.highest_index = 0
        while self.curr_leader == self.name:
            for node in self.node_list:
                msg = self.generateMessage("APPEND_RPC")
                if self.replicate_count < len(self.node_list):
                    msg = json.loads(msg.decode())
                    msg["prev_index"] = self.prev_index
                    msg["prev_term"] = (
                        self.logs[self.prev_index["term"]] if self.prev_index else None
                    )
                    msg["new_entries"] = [self.logs[self.highest_index]]
                    msg = json.dumps(msg).encode()
                else:
                    self.prev_index = self.highest_index
                    self.commit_index = self.highest_index

                if self.commit_index == self.highest_index:
                    m = self.generateMessage("COMMIT_LOG")
                    self.socket.sendto(m, (node, 5555))
                self.socket.sendto(msg, (node, 5555))
            time.sleep(0.1)

    def commitLog(self):
        if self.state == "FOLLOWER":
            if self.commit_index < self.highest_index:
                self.commit_index = self.highest_index
                print(self.name + " COMMITTED LOG")


if __name__ == "__main__":
    thisNode = Node()
    thisNode.main()
