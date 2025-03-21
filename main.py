from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import requests

app = FastAPI()

# Database setup
DATABASE_URL = "postgresql://postgres:AvFPPvjpjuhyhzAJzQbprUCXyFQsAVRo@shinkansen.proxy.rlwy.net:36120/railway"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Message Model
class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, index=True)
    sender = Column(String, index=True)
    receiver = Column(String, index=True)
    message = Column(String)

Base.metadata.create_all(bind=engine)

# Dependency for database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Check if receiver is accepted
def is_receiver_accepted(receiver: str) -> bool:
    url = f"https://loopchat-backend.vercel.app/api/accounts/accepted/{receiver}/"
    response = requests.get(url)
    return response.status_code == 200

# WebSocket Connection Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections = {}

    async def connect(self, websocket: WebSocket, sender: str, receiver: str):
        await websocket.accept()
        chat_key = frozenset([sender, receiver])
        if chat_key not in self.active_connections:
            self.active_connections[chat_key] = []
        self.active_connections[chat_key].append(websocket)

    def disconnect(self, websocket: WebSocket, sender: str, receiver: str):
        chat_key = frozenset([sender, receiver])
        self.active_connections[chat_key].remove(websocket)
        if not self.active_connections[chat_key]:
            del self.active_connections[chat_key]

    async def broadcast(self, sender: str, receiver: str, message: str):
        chat_key = frozenset([sender, receiver])
        if chat_key in self.active_connections:
            for connection in self.active_connections[chat_key]:
                await connection.send_text(f"{sender}: {message}")

manager = ConnectionManager()

@app.get("/")
async def get():
    return HTMLResponse(html)

# WebSocket endpoint
@app.websocket("/ws/{sender}/{receiver}")
async def websocket_endpoint(websocket: WebSocket, sender: str, receiver: str, db: Session = Depends(get_db)):
    if not is_receiver_accepted(receiver):
        await websocket.close(code=4000)
        return

    await manager.connect(websocket, sender, receiver)
    try:
        while True:
            data = await websocket.receive_text()
            db_message = Message(sender=sender, receiver=receiver, message=data)
            db.add(db_message)
            db.commit()
            await manager.broadcast(sender, receiver, data)
    except WebSocketDisconnect:
        manager.disconnect(websocket, sender, receiver)

# Fetch chat history
@app.get("/messages/{sender}/{receiver}")
async def get_chat_history(sender: str, receiver: str, db: Session = Depends(get_db)):
    messages = db.query(Message).filter(
        ((Message.sender == sender) & (Message.receiver == receiver)) |
        ((Message.sender == receiver) & (Message.receiver == sender))
    ).all()
    return [{"sender": msg.sender, "message": msg.message} for msg in messages]

# Full HTML chat page
html = """
<!DOCTYPE html>
<html>
<head>
    <title>WebSocket Chat</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        .chat-container {
            display: flex;
            flex-direction: column;
            height: 100vh;
            background-color: #f0f0f0;
        }
        .chat-header {
            background-color: #007bff;
            padding: 15px;
            text-align: center;
            color: white;
            font-size: 24px;
        }
        #messages {
            flex-grow: 1;
            overflow-y: auto;
            padding: 15px;
            background-color: #e9ecef;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }
        .message { display: flex; padding: 5px; }
        .sender {
            background-color: #cce5ff;
            padding: 12px;
            margin-left: auto;
            border-radius: 10px;
            max-width: 65%;
            word-wrap: break-word;
        }
        .receiver {
            background-color: #ffffff;
            padding: 12px;
            margin-right: auto;
            border-radius: 10px;
            max-width: 65%;
            word-wrap: break-word;
        }
        .input-area {
            display: flex;
            padding: 10px;
            background-color: #f8f9fa;
            border-top: 1px solid #ccc;
        }
        #messageText { width: 85%; padding: 10px; border-radius: 5px; border: 1px solid #ccc; }
        .send-button {
            width: 12%;
            background-color: #28a745;
            color: white;
            padding: 10px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
        }
        .send-button:hover { background-color: #218838; }
    </style>
</head>
<body>
    <div class="chat-container">
        <div class="chat-header">FastAPI WebSocket Chat</div>
        <ul id="messages"></ul>
        <div class="input-area">
            <input type="text" id="messageText" autocomplete="off"/>
            <button class="send-button" onclick="sendMessage(event)">Send</button>
        </div>
    </div>

    <script>
    let username = prompt("Enter your username:");
    let receiver = prompt("Enter the username of the person you want to chat with:");
    document.querySelector('.chat-header').textContent = "Chat with " + receiver;

    async function loadMessages() {
        const response = await fetch(`/messages/${username}/${receiver}`);
        const messages = await response.json();
        const messagesList = document.getElementById("messages");
        messagesList.innerHTML = "";
        messages.forEach(msg => {
            const messageElement = document.createElement("li");
            messageElement.classList.add("message");
            const messageDiv = document.createElement("div");
            messageDiv.textContent = msg.message;
            messageDiv.classList.add(msg.sender === username ? "sender" : "receiver");
            messageElement.appendChild(messageDiv);
            messagesList.appendChild(messageElement);
        });
        messagesList.scrollTop = messagesList.scrollHeight;
    }

    var ws = new WebSocket(`wss://your-app-name.onrender.com/ws/${username}/${receiver}`);

    ws.onmessage = function(event) {
        var messages = document.getElementById('messages');
        var messageElement = document.createElement('li');
        messageElement.classList.add("message");
        var messageDiv = document.createElement("div");
        messageDiv.textContent = event.data;
        messageDiv.classList.add(event.data.includes(username) ? "sender" : "receiver");
        messageElement.appendChild(messageDiv);
        messages.appendChild(messageElement);
        messages.scrollTop = messages.scrollHeight;
    };

    ws.onopen = function() { loadMessages(); };
    ws.onclose = function(event) { if (event.code === 4000) alert("Receiver not accepted."); };

    function sendMessage(event) {
        var input = document.getElementById("messageText");
        if (input.value.trim() !== "") { ws.send(input.value); input.value = ''; }
        event.preventDefault();
    }

    loadMessages();
    </script>
</body>
</html>
"""
