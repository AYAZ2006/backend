from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
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

# Dependency for getting the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Function to check if receiver is in the accepted list
def is_receiver_accepted(receiver: str) -> bool:
    url = f"https://loopchat-backend.vercel.app/api/accounts/accepted/{receiver}/"
    response = requests.get(url)
    if response.status_code == 200:
        return True  # Receiver is accepted
    return False  # Receiver is not accepted

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

# WebSocket endpoint for handling chat with receiver validation
@app.websocket("/ws/{sender}/{receiver}")
async def websocket_endpoint(websocket: WebSocket, sender: str, receiver: str, db: Session = Depends(get_db)):
    # Check if receiver is accepted
    if not is_receiver_accepted(receiver):
        await websocket.close(code=4000)  # Close the connection if receiver is not valid
        return

    # Proceed with WebSocket connection if receiver is valid
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
            margin-bottom: 20px;
            list-style: none;
            background-color: #e9ecef;
            height: 75vh;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }

        .message {
            display: flex;
            padding: 5px;
            margin: 5px 0;
        }

        .sender {
            background-color: #cce5ff;
            padding: 12px;
            margin-left: auto;
            border-radius: 10px;
            max-width: 65%;
            word-wrap: break-word;
            text-align: right;
        }

        .receiver {
            background-color: #ffffff;
            padding: 12px;
            margin-right: auto;
            border-radius: 10px;
            max-width: 65%;
            word-wrap: break-word;
            text-align: left;
        }

        .input-area {
            display: flex;
            justify-content: space-between;
            padding: 10px;
            background-color: #f8f9fa;
            border-top: 1px solid #ccc;
        }

        #messageText {
            width: 85%;
            padding: 10px;
            border-radius: 5px;
            border: 1px solid #ccc;
        }

        .send-button {
            width: 12%;
            background-color: #28a745;
            color: white;
            padding: 10px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
        }

        .send-button:hover {
            background-color: #218838;
        }

        .username {
            text-align: right;
            background-color: #cce5ff;
            padding: 8px;
            font-weight: bold;
            margin-top: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
        }

        .message-time {
            font-size: 0.75em;
            color: #6c757d;
            text-align: right;
            margin-top: 5px;
        }
    </style>
</head>
<body>
    <div class="chat-container">
        <div class="chat-header">
            <h1>FastAPI WebSocket Chat</h1>
            <h2>Chat with: <span id="chat-with"></span></h2>
        </div>

        <ul id="messages">
            <!-- Messages will be added dynamically -->
        </ul>

        <div class="input-area">
            <input type="text" class="form-control" id="messageText" autocomplete="off"/>
            <button class="send-button" onclick="sendMessage(event)">Send</button>
        </div>

        <div class="username" id="username-container"></div>
    </div>

    <script>
    let username = prompt("Enter your username:");
    let receiver = prompt("Enter the username of the person you want to chat with:");
    document.getElementById("chat-with").textContent = receiver;
    document.getElementById("username-container").textContent = "Sender: " + username;

    // Function to load chat history from the backend
    async function loadMessages() {
        const response = await fetch(`/messages/${username}/${receiver}`);
        const messages = await response.json();
        const messagesList = document.getElementById("messages");
        messagesList.innerHTML = ""; // Clear existing messages

        messages.forEach(msg => {
            const messageElement = document.createElement("li");
            messageElement.classList.add("message");

            // Display sender's message on the right, receiver's message on the left
            if (msg.sender === username) {
                const senderDiv = document.createElement("div");
                senderDiv.classList.add("sender");
                senderDiv.textContent = msg.message;
                messageElement.appendChild(senderDiv);
            } else {
                const receiverDiv = document.createElement("div");
                receiverDiv.classList.add("receiver");
                receiverDiv.textContent = msg.message;
                messageElement.appendChild(receiverDiv);
            }

            messagesList.appendChild(messageElement);
        });

        // Scroll to the bottom of the message list to show the latest message
        messagesList.scrollTop = messagesList.scrollHeight;
    }

    // WebSocket connection
    var ws = new WebSocket(`ws://localhost:8000/ws/${username}/${receiver}`);

    ws.onmessage = function(event) {
        var messages = document.getElementById('messages');
        var messageElement = document.createElement('li');
        messageElement.classList.add("message");

        // Display message from the sender or receiver based on who sent it
        if (event.data.includes(username)) {
            const senderDiv = document.createElement("div");
            senderDiv.classList.add("sender");
            senderDiv.textContent = event.data;
            messageElement.appendChild(senderDiv);
        } else {
            const receiverDiv = document.createElement("div");
            receiverDiv.classList.add("receiver");
            receiverDiv.textContent = event.data;
            messageElement.appendChild(receiverDiv);
        }

        messages.appendChild(messageElement);
        messages.scrollTop = messages.scrollHeight;  // Scroll to the latest message
    };

    ws.onopen = function() {
        console.log("WebSocket connected!");
        loadMessages();  // Load previous messages after WebSocket connects
    };

    ws.onerror = function(event) {
    console.error("WebSocket connection failed", event);
    alert("WebSocket connection failed. Please check the server or your network.");
};


    ws.onclose = function(event) {
        if (event.code === 4000) {
            alert("Receiver not accepted.");
        }
    };

    function sendMessage(event) {
        var input = document.getElementById("messageText");
        if (input.value.trim() !== "") {
            ws.send(input.value);
            input.value = '';  // Clear the input field
        }
        event.preventDefault();
    }

    loadMessages();  // Load messages on page load
</script>
</body>
</html>
"""
