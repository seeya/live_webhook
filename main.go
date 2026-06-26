package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type WebhookEvent struct {
	ID        int               `json:"id"`
	Timestamp string            `json:"timestamp"`
	Method    string            `json:"method"`
	Path      string            `json:"path"`
	Headers   map[string]string `json:"headers"`
	Query     string            `json:"query"`
	Body      string            `json:"body"`
	RemoteIP  string            `json:"remote_ip"`
}

var (
	sseClients   = make(map[chan []byte]struct{})
	sseClientsMu sync.Mutex
	eventCounter int
	eventHistory []WebhookEvent
	historyMu    sync.Mutex
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleRoot)
	mux.HandleFunc("/events", handleSSE)
	mux.HandleFunc("/history", handleHistory)
	mux.HandleFunc("/webhook/", handleWebhook)
	mux.HandleFunc("/webhook", handleWebhook)

	addr := ":9090"
	log.Printf("Webhook listener running on http://localhost%s", addr)
	log.Printf("Send webhooks to http://localhost%s/webhook", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func handleWebhook(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()

	headers := make(map[string]string)
	for k, v := range r.Header {
		headers[k] = v[0]
	}

	eventCounter++
	remoteIP := r.Header.Get("X-Forwarded-For")
	if remoteIP == "" {
		remoteIP = r.Header.Get("X-Real-IP")
	}
	if remoteIP == "" {
		remoteIP = r.RemoteAddr
	}
	event := WebhookEvent{
		ID:        eventCounter,
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Method:    r.Method,
		Path:      r.URL.Path,
		Headers:   headers,
		Query:     r.URL.RawQuery,
		Body:      string(body),
		RemoteIP:  remoteIP,
	}

	historyMu.Lock()
	eventHistory = append(eventHistory, event)
	if len(eventHistory) > 200 {
		eventHistory = eventHistory[len(eventHistory)-200:]
	}
	historyMu.Unlock()

	data, _ := json.Marshal(event)
	broadcast(data)

	log.Printf("[%s] %s %s", event.Method, event.Path, event.RemoteIP)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"received","id":%d}`, event.ID)
}

func handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch := make(chan []byte, 16)
	sseClientsMu.Lock()
	sseClients[ch] = struct{}{}
	sseClientsMu.Unlock()

	defer func() {
		sseClientsMu.Lock()
		delete(sseClients, ch)
		sseClientsMu.Unlock()
	}()

	// Send initial keepalive
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	for {
		select {
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func handleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		historyMu.Lock()
		eventHistory = nil
		eventCounter = 0
		historyMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true}`)
		return
	}

	historyMu.Lock()
	data, _ := json.Marshal(eventHistory)
	historyMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(data)
}

func broadcast(data []byte) {
	sseClientsMu.Lock()
	defer sseClientsMu.Unlock()
	for ch := range sseClients {
		select {
		case ch <- data:
		default:
		}
	}
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		handleWebhook(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, pageHTML)
}

const pageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Webhook Listener</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e1e4e8; }
.header { padding: 16px 24px; background: #161b22; border-bottom: 1px solid #30363d; display: flex; align-items: center; justify-content: space-between; }
.header h1 { font-size: 1.2rem; color: #58a6ff; }
.header .status { display: flex; align-items: center; gap: 8px; font-size: 0.8rem; color: #8b949e; }
.header .dot { width: 8px; height: 8px; border-radius: 50%; background: #da3633; }
.header .dot.connected { background: #3fb950; }
.header .url { font-family: monospace; font-size: 0.8rem; background: #21262d; padding: 4px 10px; border-radius: 4px; color: #c9d1d9; }
.header .count { font-size: 0.8rem; color: #8b949e; }

.layout { display: flex; height: calc(100vh - 57px); }
.event-list { width: 360px; min-width: 360px; border-right: 1px solid #30363d; overflow-y: auto; background: #0d1117; }
.event-detail { flex: 1; overflow-y: auto; padding: 20px; }

.event-item { padding: 10px 16px; border-bottom: 1px solid #21262d; cursor: pointer; transition: background 0.15s; }
.event-item:hover { background: #161b22; }
.event-item.active { background: #1c2a3a; border-left: 3px solid #58a6ff; }
.event-item .top-row { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
.event-item .method { font-weight: 700; font-size: 0.75rem; padding: 2px 6px; border-radius: 3px; }
.method-GET { background: #1f6b2b; color: #7ee787; }
.method-POST { background: #1f3d6b; color: #79b8ff; }
.method-PUT { background: #6b4c1f; color: #e7c87e; }
.method-DELETE { background: #6b1f1f; color: #ff7b72; }
.method-PATCH { background: #3d1f6b; color: #b895ff; }
.event-item .path { font-family: monospace; font-size: 0.8rem; color: #c9d1d9; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.event-item .time { font-size: 0.65rem; color: #484f58; margin-left: auto; white-space: nowrap; }
.event-item .ip { font-size: 0.65rem; color: #484f58; }

.detail-empty { display: flex; align-items: center; justify-content: center; height: 100%; color: #484f58; font-style: italic; }
.detail-section { margin-bottom: 20px; }
.detail-section h3 { font-size: 0.85rem; color: #8b949e; margin-bottom: 8px; border-bottom: 1px solid #21262d; padding-bottom: 6px; }
.detail-meta { display: grid; grid-template-columns: auto 1fr; gap: 4px 16px; font-size: 0.8rem; }
.detail-meta .label { color: #8b949e; }
.detail-meta .value { color: #c9d1d9; font-family: monospace; word-break: break-all; }

.headers-table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
.headers-table th { text-align: left; color: #8b949e; padding: 4px 8px; border-bottom: 1px solid #30363d; font-weight: 500; }
.headers-table td { padding: 4px 8px; border-bottom: 1px solid #21262d; font-family: monospace; word-break: break-all; }
.headers-table td:first-child { color: #79c0ff; white-space: nowrap; }
.headers-table td:last-child { color: #c9d1d9; }

.body-content { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 12px; font-family: monospace; font-size: 0.8rem; white-space: pre-wrap; word-break: break-all; color: #c9d1d9; max-height: 400px; overflow: auto; }
.body-empty { color: #484f58; font-style: italic; }

.clear-btn { background: none; border: 1px solid #30363d; color: #8b949e; padding: 4px 12px; border-radius: 4px; font-size: 0.75rem; cursor: pointer; }
.clear-btn:hover { border-color: #da3633; color: #da3633; }

.json-key { color: #79c0ff; }
.json-string { color: #a5d6ff; }
.json-number { color: #d2a8ff; }
.json-boolean { color: #ff7b72; }
.json-null { color: #8b949e; }
</style>
</head>
<body>
<div class="header">
    <h1>Webhook Listener</h1>
    <span class="url" id="webhook-url"></span>
    <span class="count" id="event-count">0 events</span>
    <div class="status">
        <span class="dot" id="status-dot"></span>
        <span id="status-text">Connecting...</span>
    </div>
    <button class="clear-btn" onclick="clearEvents()">Clear</button>
    <span class="storage-info"><span id="storage-size">0 KB</span></span>
    <button class="clear-btn" onclick="clearStorage()">Clear Storage</button>
</div>
<div class="layout">
    <div class="event-list" id="event-list"></div>
    <div class="event-detail" id="event-detail">
        <div class="detail-empty">Waiting for webhooks...</div>
    </div>
</div>

<script>
var STORAGE_KEY = 'webhook_events';
var events = loadFromStorage();
var selectedId = -1;
var evtSource = null;
var reconnectTimer = null;

document.getElementById('webhook-url').textContent = location.origin + '/webhook';

function connectSSE() {
    if (evtSource) {
        evtSource.onopen = null;
        evtSource.onmessage = null;
        evtSource.onerror = null;
        evtSource.close();
        evtSource = null;
    }
    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
    evtSource = new EventSource('/events');
    evtSource.onopen = function() {
        document.getElementById('status-dot').className = 'dot connected';
        document.getElementById('status-text').textContent = 'Connected';
    };
    evtSource.onmessage = function(e) {
        var evt = JSON.parse(e.data);
        events.unshift(evt);
        if (events.length > 200) events.pop();
        saveToStorage();
        renderList();
        updateCount();
        updateStorageSize();
        if (selectedId === -1) selectEvent(evt.id);
    };
    evtSource.onerror = function() {
        document.getElementById('status-dot').className = 'dot';
        document.getElementById('status-text').textContent = 'Disconnected';
        if (!reconnectTimer) {
            reconnectTimer = setTimeout(connectSSE, 3000);
        }
    };
}

function renderList() {
    var list = document.getElementById('event-list');
    var html = '';
    for (var i = 0; i < events.length; i++) {
        var evt = events[i];
        var active = evt.id === selectedId ? ' active' : '';
        var methodClass = 'method-' + evt.method;
        var t = new Date(evt.timestamp);
        var timeStr = t.toLocaleTimeString();
        html += '<div class="event-item' + active + '" onclick="selectEvent(' + evt.id + ')">';
        html += '<div class="top-row">';
        html += '<span class="method ' + methodClass + '">' + evt.method + '</span>';
        html += '<span class="path">' + escapeHtml(evt.path) + (evt.query ? '?' + escapeHtml(evt.query) : '') + '</span>';
        html += '<span class="time">' + timeStr + '</span>';
        html += '</div>';
        html += '<span class="ip">' + escapeHtml(evt.remote_ip) + '</span>';
        html += '</div>';
    }
    if (events.length === 0) {
        html = '<div style="padding:20px;color:#484f58;font-style:italic;text-align:center;">No webhooks received yet</div>';
    }
    list.innerHTML = html;
}

function selectEvent(id) {
    selectedId = id;
    renderList();
    var evt = null;
    for (var i = 0; i < events.length; i++) {
        if (events[i].id === id) { evt = events[i]; break; }
    }
    if (!evt) return;
    renderDetail(evt);
}

function renderDetail(evt) {
    var detail = document.getElementById('event-detail');
    var t = new Date(evt.timestamp);
    var html = '';

    html += '<div class="detail-section"><h3>Request</h3>';
    html += '<div class="detail-meta">';
    html += '<span class="label">Method</span><span class="value"><span class="method method-' + evt.method + '">' + evt.method + '</span></span>';
    html += '<span class="label">Path</span><span class="value">' + escapeHtml(evt.path) + '</span>';
    if (evt.query) html += '<span class="label">Query</span><span class="value">' + escapeHtml(evt.query) + '</span>';
    html += '<span class="label">Time</span><span class="value">' + t.toLocaleString() + '</span>';
    html += '<span class="label">Remote IP</span><span class="value">' + escapeHtml(evt.remote_ip) + '</span>';
    html += '</div></div>';

    html += '<div class="detail-section"><h3>Headers</h3>';
    var headerKeys = Object.keys(evt.headers).sort();
    if (headerKeys.length > 0) {
        html += '<table class="headers-table"><tr><th>Header</th><th>Value</th></tr>';
        for (var i = 0; i < headerKeys.length; i++) {
            html += '<tr><td>' + escapeHtml(headerKeys[i]) + '</td><td>' + escapeHtml(evt.headers[headerKeys[i]]) + '</td></tr>';
        }
        html += '</table>';
    } else {
        html += '<div class="body-empty">No headers</div>';
    }
    html += '</div>';

    html += '<div class="detail-section"><h3>Body</h3>';
    if (evt.body) {
        var formatted = tryFormatJson(evt.body);
        html += '<div class="body-content">' + formatted + '</div>';
    } else {
        html += '<div class="body-content body-empty">Empty body</div>';
    }
    html += '</div>';

    detail.innerHTML = html;
}

function tryFormatJson(str) {
    try {
        var obj = JSON.parse(str);
        return syntaxHighlight(JSON.stringify(obj, null, 2));
    } catch(e) {
        return escapeHtml(str);
    }
}

function syntaxHighlight(json) {
    json = escapeHtml(json);
    json = json.replace(/"([^"]+)"(\s*:)/g, '<span class="json-key">"$1"</span>$2');
    json = json.replace(/: "([^"]*)"/g, ': <span class="json-string">"$1"</span>');
    json = json.replace(/: (\d+\.?\d*)/g, ': <span class="json-number">$1</span>');
    json = json.replace(/: (true|false)/g, ': <span class="json-boolean">$1</span>');
    json = json.replace(/: (null)/g, ': <span class="json-null">$1</span>');
    return json;
}

function escapeHtml(str) {
    return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function updateCount() {
    document.getElementById('event-count').textContent = events.length + ' event' + (events.length !== 1 ? 's' : '');
}

function clearEvents() {
    events = [];
    selectedId = -1;
    saveToStorage();
    renderList();
    updateCount();
    updateStorageSize();
    document.getElementById('event-detail').innerHTML = '<div class="detail-empty">Waiting for webhooks...</div>';
    fetch('/history', { method: 'DELETE' });
}

function saveToStorage() {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(events)); } catch(e) {}
}

function loadFromStorage() {
    try {
        var data = localStorage.getItem(STORAGE_KEY);
        return data ? JSON.parse(data) : [];
    } catch(e) { return []; }
}

function getStorageSize() {
    try {
        var data = localStorage.getItem(STORAGE_KEY) || '';
        var bytes = new Blob([data]).size;
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / 1048576).toFixed(1) + ' MB';
    } catch(e) { return '0 B'; }
}

function updateStorageSize() {
    document.getElementById('storage-size').textContent = getStorageSize();
}

function clearStorage() {
    localStorage.removeItem(STORAGE_KEY);
    events = [];
    selectedId = -1;
    renderList();
    updateCount();
    updateStorageSize();
    document.getElementById('event-detail').innerHTML = '<div class="detail-empty">Waiting for webhooks...</div>';
}

// Load from localStorage on page load, then merge server history
if (events.length > 0) {
    renderList();
    updateCount();
}
fetch('/history').then(function(r){ return r.json(); }).then(function(data){
    if (data && data.length) {
        var existingIds = {};
        for (var i = 0; i < events.length; i++) existingIds[events[i].id] = true;
        for (var j = 0; j < data.length; j++) {
            if (!existingIds[data[j].id]) events.push(data[j]);
        }
        events.sort(function(a, b) { return b.id - a.id; });
        if (events.length > 200) events = events.slice(0, 200);
        saveToStorage();
        renderList();
        updateCount();
    }
});
updateStorageSize();
connectSSE();
</script>
</body>
</html>
`
