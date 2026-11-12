require('dotenv').config();
const path = require('node:path');
const express = require('express');
const mqtt = require('mqtt');

const url = process.env.MQTT_URL || 'mqtt://localhost:1883';
const topic = process.env.TOPIC || 'iot/demo/temperature';
const webPort = Number(process.env.WEB_PORT) || 3000;
const MAX_POINTS = 60;

const history = [];
const sseClients = new Set();
let mqttStatus = { mqtt: 'connecting', message: '正在连接 Broker…' };

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/history', (req, res) => {
  res.json({ topic, url, status: mqttStatus, points: history });
});

app.get('/api/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
  });

  sendEvent(res, 'meta', { topic, url });
  sendEvent(res, 'status', mqttStatus);
  sendEvent(res, 'history', history);

  sseClients.add(res);
  const heartbeat = setInterval(() => res.write(': ping\n\n'), 15000);
  req.on('close', () => {
    clearInterval(heartbeat);
    sseClients.delete(res);
  });
});

function sendEvent(res, event, data) {
  res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

function broadcast(event, data) {
  for (const client of sseClients) sendEvent(client, event, data);
}

function setStatus(status) {
  mqttStatus = status;
  broadcast('status', status);
}

const client = mqtt.connect(url, {
  username: process.env.MQTT_USERNAME || undefined,
  password: process.env.MQTT_PASSWORD || undefined,
});

let publishTimer = null;

client.on('connect', () => {
  console.log('[PUB] Connected:', url);
  setStatus({ mqtt: 'connected', message: `已连接 ${url}` });

  if (!publishTimer) {
    publishTimer = setInterval(publishReading, 2000);
  }
});

// 模拟设备上报温度
function publishReading() {
  const data = {
    deviceId: 'sensor-001',
    ts: Date.now(),
    temperature: (20 + Math.random() * 5).toFixed(2),
    humidity: (40 + Math.random() * 10).toFixed(2),
  };
  const payload = JSON.stringify(data);

  client.publish(topic, payload, { qos: 0 }, (err) => {
    if (err) {
      console.error('[PUB] Publish error:', err.message);
      return;
    }
    console.log('[PUB] Sent:', payload);

    const point = {
      deviceId: data.deviceId,
      ts: data.ts,
      temperature: Number(data.temperature),
      humidity: Number(data.humidity),
    };
    history.push(point);
    if (history.length > MAX_POINTS) history.shift();
    broadcast('reading', point);
  });
}

client.on('error', (err) => {
  console.error('[PUB] Error:', err.message);
  setStatus({ mqtt: 'error', message: err.message });
});

client.on('close', () => setStatus({ mqtt: 'disconnected', message: '连接已断开，正在重连…' }));
client.on('reconnect', () => setStatus({ mqtt: 'connecting', message: '正在重连 Broker…' }));

app.listen(webPort, () => {
  console.log(`[WEB] Dashboard: http://localhost:${webPort}`);
});
