from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import json

app = FastAPI()

# Database setup
DATABASE_URL = "sqlite:///./chat.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
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

# Dependency for getting the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# WebSocket Connection Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections = {}  # Dictionary to store active chats

    async def connect(self, websocket: WebSocket, sender: str, receiver: str):
        await websocket.accept()
        chat_key = frozenset([sender, receiver])  # Unique key for the chat
        if chat_key not in self.active_connections:
            self.active_connections[chat_key] = []
        self.active_connections[chat_key].append(websocket)

    def disconnect(self, websocket: WebSocket, sender: str, receiver: str):
        chat_key = frozenset([sender, receiver])
        self.active_connections[chat_key].remove(websocket)
        if not self.active_connections[chat_key]:  # Remove chat if no active connections
            del self.active_connections[chat_key]

    async def broadcast(self, sender: str, receiver: str, message: str):
        chat_key = frozenset([sender, receiver])
        if chat_key in self.active_connections:
            for connection in self.active_connections[chat_key]:
                await connection.send_text(f"{sender}: {message}")

manager = ConnectionManager()

# Serve the chat page
@app.get("/")
async def get():
    return HTMLResponse(html)

# WebSocket endpoint for handling chat
@app.websocket("/ws/{sender}/{receiver}")
async def websocket_endpoint(websocket: WebSocket, sender: str, receiver: str, db: Session = Depends(get_db)):
    await manager.connect(websocket, sender, receiver)
    try:
        while True:
            data = await websocket.receive_text()
            # Store message in database
            db_message = Message(sender=sender, receiver=receiver, message=data)
            db.add(db_message)
            db.commit()
            await manager.broadcast(sender, receiver, data)
    except WebSocketDisconnect:
        manager.disconnect(websocket, sender, receiver)

# API to fetch previous messages
@app.get("/messages/{sender}/{receiver}")
async def get_chat_history(sender: str, receiver: str, db: Session = Depends(get_db)):
    messages = db.query(Message).filter(
        ((Message.sender == sender) & (Message.receiver == receiver)) |
        ((Message.sender == receiver) & (Message.receiver == sender))
    ).all()
    
    return [{"sender": msg.sender, "message": msg.message} for msg in messages]

# HTML page with JavaScript WebSocket chat client
html = """
<!DOCTYPE html>
<html>
<head>
    <title>WebSocket Chat</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-3">
        <h1>FastAPI WebSocket Chat</h1>
        <h2>Chat with: <span id="chat-with"></span></h2>
        <form onsubmit="sendMessage(event)">
            <input type="text" class="form-control" id="messageText" autocomplete="off"/>
            <button class="btn btn-outline-primary mt-2">Send</button>
        </form>
        <ul id="messages" class="mt-5"></ul>
    </div>

    <script>
        let username = prompt("Enter your username:");
        let receiver = prompt("Enter the username of the person you want to chat with:");
        document.getElementById("chat-with").textContent = receiver;

        async function loadMessages() {
            const response = await fetch(`/messages/${username}/${receiver}`);
            const messages = await response.json();
            const messagesList = document.getElementById("messages");
            messagesList.innerHTML = "";

            messages.forEach(msg => {
                const messageElement = document.createElement("li");
                messageElement.textContent = msg.sender + ": " + msg.message;
                messagesList.appendChild(messageElement);
            });
        }

        var ws = new WebSocket(`ws://localhost:8000/ws/${username}/${receiver}`);
        
        ws.onmessage = function(event) {
            var messages = document.getElementById('messages');
            var message = document.createElement('li');
            message.textContent = event.data;
            messages.appendChild(message);
        };

        ws.onopen = function() {
            console.log("WebSocket connected!");
            loadMessages();  // Load previous messages after WebSocket connects
        };

        function sendMessage(event) {
            var input = document.getElementById("messageText");
            if (input.value.trim() !== "") {
                ws.send(input.value);
                input.value = '';
            }
            event.preventDefault();
        }

        loadMessages();  // Load messages on page load
    </script>
</body>
</html>
"""
