# Real-Time Data Pipeline (WebSockets + LangGraph)

This project simulates a simple real-time data pipeline:
- `server.py` generates and streams events over WebSockets.
- `client.py` receives events and runs them through a LangGraph pipeline.
- `pipeline.py` handles ingest, validate, transform, and load steps.
- `utils.py` creates fake events with occasional bad data.

## 1. Install dependencies

```powershell
pip install -r requirements.txt
```

If `pip` is not available directly, use:

```powershell
python -m pip install -r requirements.txt
```

## 2. Start the WebSocket server

Open terminal 1 in the project folder and run:

```powershell
python server.py
```

Expected output:
- A startup line like `WebSocket server running at ws://localhost:8765`
- Continuous `Sent: {...}` event logs

## 3. Start the client

Open terminal 2 in the same project folder and run:

```powershell
python client.py
```

Expected output:
- `Connected to ws://localhost:8765`
- Pipeline logs such as:
  - `Loaded: {...}` for valid events
  - `Validation failed...` / `Load skipped...` for invalid events

## 4. Stop the programs

Press `Ctrl + C` in each terminal.

## Optional: Python path fallback (Windows)

If `python` does not work in your shell, use your full Python path:

```powershell
& 'C:\Users\Dhruv Agarwal\AppData\Local\Programs\Python\Python314\python.exe' server.py
& 'C:\Users\Dhruv Agarwal\AppData\Local\Programs\Python\Python314\python.exe' client.py
```
