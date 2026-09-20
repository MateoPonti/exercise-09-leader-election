"""
Bully algorithm for leader election among Node Registry instances.

Each running instance has a numeric NODE_ID (read from the environment).
The node with the HIGHEST id always wins an election ("the bully").
Peers are given via the PEERS environment variable as a comma separated
list of base URLs, e.g.:

    PEERS=http://node-2:8080,http://node-3:8080

Because PEERS only carries URLs (not ids), each node discovers a peer's id
by calling that peer's `GET /election/id` endpoint the first time it needs
it, and caches the result.

Algorithm (Garcia-Molina):
1. A node that notices the leader is unreachable calls `start_election()`.
2. It sends an ELECTION message to every peer with a HIGHER id
   (`handle_election_message` on the receiving side).
     - If nobody with a higher id answers, this node wins the election
       and calls `declare_victory()`.
     - If at least one higher node answers OK, this node steps back and
       waits to be told who won (`handle_coordinator_message`).
3. A node that receives an ELECTION message replies OK and, if it is not
   already running an election itself, starts one (it necessarily
   outranks the sender, since ELECTION messages only go "upward").
4. The winner sends a COORDINATOR message to every other node.
5. `heartbeat_check()` is meant to be called periodically (e.g. from a
   background thread) so every node keeps verifying that the leader it
   knows about is still alive, triggering a new election otherwise.
"""

import logging
import os
import threading
import time
from typing import Dict, Optional

import requests

logger = logging.getLogger("election")

# How long we wait for a higher node to answer an ELECTION message.
ELECTION_TIMEOUT = float(os.environ.get("ELECTION_TIMEOUT", "2"))
# How often heartbeat_loop() checks that the leader is still alive.
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "3"))
# Generic timeout used for any other node-to-node HTTP call.
REQUEST_TIMEOUT = float(os.environ.get("ELECTION_REQUEST_TIMEOUT", "2"))


class ElectionState:
    """Thread-safe state shared by the election logic and the API layer."""

    def __init__(self, node_id: int, peer_urls: Dict[str, Optional[int]]):
        self.node_id = node_id
        # {base_url: peer_id or None if not yet discovered}
        self.peer_urls: Dict[str, Optional[int]] = peer_urls
        self.leader_id: Optional[int] = None
        self.election_in_progress = False
        self.lock = threading.RLock()
        self.stop_heartbeat = threading.Event()

    # -- peer helpers ------------------------------------------------
    def known_peers(self) -> Dict[int, str]:
        """Return {peer_id: url} for peers whose id we already know."""
        with self.lock:
            return {pid: url for url, pid in self.peer_urls.items() if pid is not None}

    def set_peer_id(self, url: str, peer_id: int) -> None:
        with self.lock:
            self.peer_urls[url] = peer_id

    def peer_url_list(self):
        with self.lock:
            return list(self.peer_urls.keys())


state: Optional[ElectionState] = None


def _parse_peers_env() -> Dict[str, Optional[int]]:
    """Parse PEERS ("http://node-2:8080,http://node-3:8080") into a dict."""
    raw = os.environ.get("PEERS", "").strip()
    peers: Dict[str, Optional[int]] = {}
    if not raw:
        return peers
    for entry in raw.split(","):
        url = entry.strip().rstrip("/")
        if not url:
            continue
        if "=" in url:
            # Also accept an explicit "id=url" form, in case it's provided.
            pid_str, real_url = url.split("=", 1)
            try:
                peers[real_url.strip().rstrip("/")] = int(pid_str.strip())
                continue
            except ValueError:
                pass
        peers[url] = None
    return peers


def init_state(node_id: Optional[int] = None) -> ElectionState:
    """Build (or rebuild) the global election state from the environment."""
    global state
    nid = node_id if node_id is not None else int(os.environ.get("NODE_ID", "0"))
    state = ElectionState(nid, _parse_peers_env())
    logger.info("Election state initialised: node_id=%s peers=%s", nid, list(state.peer_urls))
    return state


def get_state() -> ElectionState:
    global state
    if state is None:
        init_state()
    return state


def _discover_peer_id(url: str) -> Optional[int]:
    """Ask a peer for its NODE_ID and cache it."""
    st = get_state()
    try:
        resp = requests.get(f"{url}/election/id", timeout=REQUEST_TIMEOUT)
        if resp.ok:
            pid = int(resp.json()["node_id"])
            st.set_peer_id(url, pid)
            return pid
    except Exception as exc:
        logger.info("Could not discover id for peer %s: %s", url, exc)
    return None


def _peers_with_ids() -> Dict[int, str]:
    """Return {peer_id: url} for every peer, discovering unknown ids."""
    st = get_state()
    for url in st.peer_url_list():
        if st.peer_urls.get(url) is None:
            _discover_peer_id(url)
    return st.known_peers()


# --------------------------------------------------------------------------
# Core Bully algorithm
# --------------------------------------------------------------------------

def start_election() -> dict:
    """
    Initiate an election: send an ELECTION message to every known peer with
    a higher id. If none of them answers, this node wins immediately.
    """
    st = get_state()
    with st.lock:
        if st.election_in_progress:
            return {"status": "already_in_progress", "node_id": st.node_id}
        st.election_in_progress = True

    logger.info("Node %s starting an election", st.node_id)
    higher = {pid: url for pid, url in _peers_with_ids().items() if pid > st.node_id}

    if not higher:
        # Nobody with a higher id is known/alive: we win by default.
        return declare_victory()

    responded = False
    for pid, url in higher.items():
        try:
            resp = requests.post(
                f"{url}/election/message",
                json={"from_id": st.node_id},
                timeout=ELECTION_TIMEOUT,
            )
            if resp.ok:
                responded = True
        except Exception as exc:
            logger.info("Peer %s (%s) did not answer ELECTION: %s", pid, url, exc)

    if not responded:
        # Nobody higher is alive: we win.
        return declare_victory()

    # Someone higher answered OK: step back and wait for their COORDINATOR
    # message. If it never arrives, the heartbeat loop will retry later.
    with st.lock:
        st.election_in_progress = False
    logger.info(
        "Node %s stepped back; waiting for a higher node to declare victory",
        st.node_id,
    )
    return {"status": "deferred", "node_id": st.node_id}


def handle_election_message(sender_id: int) -> dict:
    """
    Handle an incoming ELECTION message from a lower-id node: reply OK and,
    if we are not already electing, start our own election (we necessarily
    outrank the sender, since ELECTION only travels to higher ids).
    """
    st = get_state()
    logger.info("Node %s received ELECTION from %s", st.node_id, sender_id)

    with st.lock:
        already_running = st.election_in_progress

    if not already_running:
        threading.Thread(target=start_election, daemon=True).start()

    return {"status": "ok", "node_id": st.node_id}


def declare_victory() -> dict:
    """Announce self as leader to every other known peer."""
    st = get_state()
    with st.lock:
        st.leader_id = st.node_id
        st.election_in_progress = False

    logger.info("Node %s declares itself the leader", st.node_id)

    for pid, url in _peers_with_ids().items():
        try:
            requests.post(
                f"{url}/election/coordinator",
                json={"leader_id": st.node_id},
                timeout=REQUEST_TIMEOUT,
            )
        except Exception as exc:
            logger.info("Could not notify peer %s (%s) of victory: %s", pid, url, exc)

    return {"status": "leader", "leader_id": st.node_id}


def handle_coordinator_message(leader_id: int) -> dict:
    """Handle an incoming COORDINATOR message: accept the new leader."""
    st = get_state()
    with st.lock:
        st.leader_id = leader_id
        st.election_in_progress = False
    logger.info("Node %s acknowledges node %s as the new leader", st.node_id, leader_id)
    return {"status": "ack", "leader_id": leader_id}


def heartbeat_check() -> dict:
    """
    Verify that the current leader is alive. If it is not (or no leader is
    known yet), start a new election.
    """
    st = get_state()
    with st.lock:
        leader_id = st.leader_id
        my_id = st.node_id

    if leader_id == my_id:
        return {"status": "self_leader", "leader_id": leader_id}

    if leader_id is None:
        start_election()
        return {"status": "election_started", "reason": "no_leader"}

    url = _peers_with_ids().get(leader_id)
    if url is None:
        with st.lock:
            st.leader_id = None
        start_election()
        return {"status": "election_started", "reason": "unknown_leader"}

    try:
        resp = requests.get(f"{url}/health", timeout=REQUEST_TIMEOUT)
        if resp.ok:
            return {"status": "leader_alive", "leader_id": leader_id}
    except Exception as exc:
        logger.info("Leader %s unreachable: %s", leader_id, exc)

    with st.lock:
        st.leader_id = None
    start_election()
    return {"status": "election_started", "reason": "leader_unreachable"}


def heartbeat_loop(interval: float = HEARTBEAT_INTERVAL, stop_event: Optional[threading.Event] = None) -> None:
    """Background loop that periodically calls heartbeat_check()."""
    st = get_state()
    ev = stop_event or st.stop_heartbeat
    while not ev.is_set():
        try:
            heartbeat_check()
        except Exception:
            logger.exception("heartbeat_check failed")
        ev.wait(interval)


def bootstrap_election(retries: int = 6, delay: float = 1.5) -> None:
    """
    Kick off an initial election shortly after startup, retrying a few times.
    Sibling node containers may still be starting up (DNS not yet resolvable,
    connection refused, etc.), so a single early attempt can wrongly make a
    node declare itself leader before a higher-id node is reachable. Once
    that higher node boots and runs its own election, it will broadcast
    COORDINATOR and correct everyone -- but retrying here gets the cluster
    to agreement much faster than waiting for the first heartbeat tick.
    """
    st = get_state()
    if not st.peer_url_list():
        # Single-node deployment: nothing to elect against.
        declare_victory()
        return

    def _run():
        for attempt in range(retries):
            time.sleep(delay)
            with st.lock:
                already_have_leader = st.leader_id is not None
            if already_have_leader:
                return
            start_election()

    threading.Thread(target=_run, daemon=True).start()


def start_heartbeat_thread() -> Optional[threading.Thread]:
    """Start the heartbeat loop in a daemon thread, if there are peers."""
    st = get_state()
    if not st.peer_url_list():
        return None
    st.stop_heartbeat.clear()
    thread = threading.Thread(target=heartbeat_loop, daemon=True)
    thread.start()
    return thread


def stop_heartbeat() -> None:
    st = get_state()
    st.stop_heartbeat.set()
