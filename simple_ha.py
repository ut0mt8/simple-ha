from threading import Thread
import socket
import SocketServer
import argparse
import signal
import logging
import subprocess
import sys
import time

ACTIVE = 'ACTIVE'
BACKUP = 'BACKUP'
state = BACKUP
params = {}


class requestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        global state
        self.request.send("HA:%s:%s:%s:"%(state, params['key'], params['prio']))
        return


def check_peer(addr):
    global state
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(addr)
        response = s.recv(100)
    except Exception as e:
        log.debug("[%s]: cannot connect to peer, %s" % (state, e))
        return 0

    responses = ()
    if response:
        log.debug('[%s]: received from peer "%s"' % (state, response))
        s.close()
        try:
            responses = response.split(':')
        except:
            log.debug("[%s]: malformed response from peer" % state)
            return 0
        if responses[0] != 'HA': 
            log.debug("[%s]: malformed response from peer, not found HA" % state)
            return 0
        if responses[3].isdigit() != True:
            log.debug("[%s]: malformed response from peer, priority not numeric %s" % (state, responses[3]))
            return 0
        if responses[2] != params['key']:
            log.debug("[%s]: received invalid key from peer" % state)
            return 0
        return int(responses[3])

    return 0


def signal_handler(signal, frame):
    print "exiting..."
    sys.exit(0)


if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument("listen_ip", help="the local ip to bind to")
    parser.add_argument("listen_port", type=int, help="the local port to bind to")
    parser.add_argument("priority", type=int, help="the local priority")
    parser.add_argument("peer_ip", help="the ip to connect to the peer")
    parser.add_argument("peer_port", type=int, help="the port to connect to the peer")
    parser.add_argument("active_script", help="the script to launch when switching from backup state to active state")
    parser.add_argument("backup_script", help="the script to launch when switching from active state to backup state")
    parser.add_argument("-v", "--verbose", default=False, action="store_true", help="be more verbose")
    parser.add_argument("-k", "--key", default='OdcejToQuor4', help="the shared key between peers")
    parser.add_argument("-r", "--retry", type=int, default=3, help="the number of time retrying connecting to the peer when is dead")
    parser.add_argument("-i", "--interval", type=int, default=2, help="the interval in second between check to the peer")
    args = parser.parse_args()

    params['prio'] = args.priority
    params['key'] = args.key

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    log = logging.getLogger(__file__)

    srv_addr = (args.listen_ip, args.listen_port)
    peer_addr = (args.peer_ip, args.peer_port)

    for retry in range(1, 10):
        try:
            server = SocketServer.TCPServer(srv_addr, requestHandler)
            t = Thread(target=server.serve_forever)
            t.setDaemon(True)
            t.start()
            log.info("listen on %s:%s, ready" % (args.listen_ip, args.listen_port))
            break
        except Exception as e:
            log.warning("cannot bind on %s:%s, %s, retrying" % (args.listen_ip, args.listen_port,e))
            time.sleep(10)

    checked = 0

    while True:
        peer = check_peer(peer_addr)
        if state == BACKUP:
            if peer == 0:
                if checked >= args.retry:
                    log.info("[%s]: peer is definitively dead, now becoming ACTIVE" % state)
                    checked = 0
                    state = ACTIVE
                    return_code = subprocess.call(args.active_script, shell=True)
                    log.info("[%s]: active_script %s return %d" % (state, args.active_script, return_code))
                else:
                    checked += 1
                    log.info("[%s]: peer is dead %s time, retrying" % (state, checked))
            elif peer < params['prio']:
                log.info("[%s]: peer is alive but with a lower prio, now becoming ACTIVE" % state)
                checked = 0
                state = ACTIVE
                return_code = subprocess.call(args.active_script, shell=True)
                log.info("[%s]: active_script %s return %d" % (state, args.active_script, return_code))
            else: 
                log.info("[%s]: peer is alive with a higher prio, doing nothing" % state)
                checked = 0
        elif state == ACTIVE: 
            if peer == 0:
                log.info("[%s]: peer is dead but we are active, doing nothing" % state)
            elif peer > params['prio']:
                log.info("[%s]: peer is alive with a higher prio, becoming BACKUP" % state)
                checked = 0
                state = BACKUP
                return_code = subprocess.call(args.backup_script, shell=True)
                log.info("[%s]: backup_script %s return %d" % (state, args.backup_script, return_code))
            else:
                log.info("[%s]: peer is alive with a lower prio, doing nothing" % state)
                checked = 0
 
        time.sleep(args.interval)
