<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>TMS9 - Premium Ticket Meta Search</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/4.6.1/css/bootstrap.min.css" />
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    <style>
        body {
            font-family: 'Poppins', sans-serif;
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            min-height: 100vh;
        }
        .container {
            max-width: 800px;
            margin-top: 50px;
        }
        .header {
            text-align: center;
            padding: 20px;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
            margin-bottom: 30px;
        }
        .header h2 {
            color: #2c3e50;
            font-weight: 600;
            margin: 0;
        }
        .chat-container {
            background: white;
            border-radius: 15px;
            padding: 20px;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
        }
        .form-group {
            position: relative;
            margin-bottom: 20px;
        }
        #question {
            border: none;
            border-radius: 25px;
            padding: 15px 25px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
            transition: all 0.3s ease;
        }
        #question:focus {
            outline: none;
            box-shadow: 0 4px 15px rgba(0, 123, 255, 0.2);
        }
        #sendBtn {
            background: linear-gradient(45deg, #007bff, #00d4ff);
            border: none;
            border-radius: 25px;
            padding: 12px 30px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
            transition: all 0.3s ease;
        }
        #sendBtn:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(0, 123, 255, 0.3);
        }
        #response {
            max-height: 400px;
            overflow-y: auto;
            margin-top: 20px;
        }
        .message {
            animation: fadeIn 0.5s ease;
            margin-bottom: 15px;
            padding: 15px;
            border-radius: 10px;
            background: #f8f9fa;
        }
        .user-message {
            background: #e3f2fd;
        }
        .ai-message {
            background: #f1f8ff;
        }
        .loading {
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 15px;
            color: #666;
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>Happy Traveling with TMS9 Premium</h2>
            <small class="text-muted">Fast. Smart. Reliable.</small>
        </div>

        <div class="chat-container">
            <div class="form-group">
                <input type="text" class="form-control" id="question" placeholder="Ask about your travel..." />
            </div>
            <button class="btn btn-success" id="sendBtn">Ask Now!</button>

            <div id="response"></div>
        </div>
    </div>

    <script>
        document.addEventListener("DOMContentLoaded", function () {
            const responseContainer = document.getElementById("response");
            const inputField = document.getElementById("question");
            const sendButton = document.getElementById("sendBtn");

            const csrfToken = "{{ csrf_token() }}"; // Laravel CSRF Token

            const sendMessage = async () => {
                let userMessage = inputField.value.trim();
                if (!userMessage) {
                    showError("Please enter a question.");
                    return;
                }

                inputField.value = "";
                showLoading();

                try {
                    const response = await fetch("{{ route('processForm') }}", {
                        method: "POST",
                        headers: {
                            "Content-Type": "application/json",
                            "X-CSRF-TOKEN": csrfToken
                        },
                        body: JSON.stringify({ question: userMessage }),
                    });

                    const data = await response.json();
                    clearLoading();

                    if (response.ok) {
                        displayResponse(userMessage, data.answer);
                    } else {
                        showError(data.error || "Failed to get response.");
                    }
                } catch (error) {
                    clearLoading();
                    showError("Error: " + error.message);
                }
            };

            const showLoading = () => {
                responseContainer.innerHTML += `
                    <div class="loading">
                        <div class="spinner-border spinner-border-sm mr-2"></div>
                        <span>Thinking...</span>
                    </div>
                `;
                responseContainer.scrollTop = responseContainer.scrollHeight;
            };

            const clearLoading = () => {
                document.querySelectorAll(".loading").forEach(el => el.remove());
            };

            const displayResponse = (userMessage, aiMessage) => {
    // If AI response is empty or just whitespace, set default message
    let formattedResponse = aiMessage && aiMessage.trim().length > 0 ? aiMessage : "I don't know.";

    responseContainer.innerHTML += `
        <div class="message user-message">
            <strong>You:</strong> ${userMessage}
        </div>
        <div class="message ai-message">
            <strong>TMS9:</strong> <div class="ai-content">${marked.parse(formattedResponse)}</div>
        </div>
    `;
    responseContainer.scrollTop = responseContainer.scrollHeight;
};


            const showError = (message) => {
                responseContainer.innerHTML += `
                    <div class="alert alert-danger">${message}</div>
                `;
                responseContainer.scrollTop = responseContainer.scrollHeight;
            };

            sendButton.addEventListener("click", sendMessage);
            inputField.addEventListener("keypress", function (event) {
                if (event.key === "Enter") {
                    event.preventDefault();
                    sendMessage();
                }
            });
        });
    </script>
</body>
</html>
