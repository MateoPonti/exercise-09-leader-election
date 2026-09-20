from datetime import datetime, timezone
from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session
from src import election
from src.database import Base, engine, get_db
from src.models import Node
from src.schemas import (
    CoordinatorMessage,
    ElectionMessage,
    NodeCreate,
    NodeResponse,
    NodeUpdate,
)

Base.metadata.create_all(bind=engine)
app = FastAPI()

@app.on_event("startup")
def on_startup():
    election.init_state()
    election.start_heartbeat_thread()
    election.bootstrap_election()

@app.on_event("shutdown")
def on_shutdown():
    election.stop_heartbeat()

@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    count = db.query(Node).filter(Node.status == "active").count()
    return {"status": "ok", "db": db_status, "nodes_count": count}

@app.get("/election/id")
def election_id():
    """Lets peers discover this node's numeric id."""
    st = election.get_state()
    return {"node_id": st.node_id}

@app.get("/election/leader")
def election_leader():
    st = election.get_state()
    return {"node_id": st.node_id, "leader_id": st.leader_id, "leader": st.leader_id}

@app.get("/leader")
def leader():
    """Plain top-level alias, in case a caller doesn't use the /election prefix."""
    st = election.get_state()
    return {"node_id": st.node_id, "leader_id": st.leader_id, "leader": st.leader_id}

@app.get("/election/status")
def election_status():
    st = election.get_state()
    return {
        "node_id": st.node_id,
        "leader_id": st.leader_id,
        "election_in_progress": st.election_in_progress,
        "peers": st.peer_url_list(),
    }

@app.post("/election/start")
def election_start():
    """Manually trigger an election (also used by tests / operators)."""
    return election.start_election()

@app.post("/election/message")
def election_message(msg: ElectionMessage):
    """Receive an ELECTION message from a lower-id node."""
    return election.handle_election_message(msg.from_id)

@app.post("/election/coordinator")
def election_coordinator(msg: CoordinatorMessage):
    """Receive a COORDINATOR message announcing the new leader."""
    return election.handle_coordinator_message(msg.leader_id)

@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    return db_node

@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    return db.query(Node).all()

@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node

@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node

@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)
