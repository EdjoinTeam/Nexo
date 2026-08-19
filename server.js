// Nexo — минимальный WebSocket relay-сервер.
//
// Что делает:
//  - принимает подключения клиентов Nexo (nexo.html);
//  - после {type:'auth', login} запоминает, какой сокет соответствует
//    какому логину (один логин может иметь несколько сокетов — вкладки,
//    устройства);
//  - на любое сообщение с полем `to` пересылает его один в один всем
//    сокетам этого логина (relay, без сохранения истории — хранение
//    сообщений в этом приложении и так идёт через отдельное shared-хранилище,
//    WS нужен только для мгновенной доставки "прямо сейчас": сам текст,
//    typing, read-receipts, звонки);
//  - рассылает всем список залогиненных логинов при каждом
//    подключении/отключении (type: 'presence'), чтобы у всех сразу
//    обновлялся статус "в сети" / "был(а) недавно".
//
// Никакой аутентификации по паролю здесь нет и не нужно — сервер доверяет
// логину, который прислал клиент (пароль уже проверен раньше, на этапе
// входа в само приложение, через отдельное хранилище пользователей).
// Это чисто транспортный релей, а не источник правды о том, кто есть кто.

const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3080;

// login (нижний регистр) -> Set<WebSocket>
const clientsByLogin = new Map();

function loginOf(ws) {
  return ws && ws.__login;
}

function currentOnlineLogins() {
  return Array.from(clientsByLogin.keys());
}

function broadcastPresence() {
  const payload = JSON.stringify({ type: 'presence', online: currentOnlineLogins() });
  for (const sockets of clientsByLogin.values()) {
    for (const sock of sockets) {
      if (sock.readyState === sock.OPEN) sock.send(payload);
    }
  }
}

function registerLogin(ws, login) {
  const lc = String(login || '').toLowerCase().trim();
  if (!lc) return;
  ws.__login = lc;
  if (!clientsByLogin.has(lc)) clientsByLogin.set(lc, new Set());
  clientsByLogin.get(lc).add(ws);
}

function unregisterLogin(ws) {
  const lc = loginOf(ws);
  if (!lc) return;
  const set = clientsByLogin.get(lc);
  if (!set) return;
  set.delete(ws);
  if (set.size === 0) clientsByLogin.delete(lc);
}

function sendTo(login, obj) {
  const lc = String(login || '').toLowerCase().trim();
  const set = clientsByLogin.get(lc);
  if (!set || set.size === 0) return false;
  const payload = JSON.stringify(obj);
  let delivered = false;
  for (const sock of set) {
    if (sock.readyState === sock.OPEN) {
      sock.send(payload);
      delivered = true;
    }
  }
  return delivered;
}

const wss = new WebSocketServer({ port: PORT });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (!msg || typeof msg !== 'object') return;

    if (msg.type === 'auth') {
      registerLogin(ws, msg.login);
      ws.send(JSON.stringify({ type: 'auth_ok', online: currentOnlineLogins() }));
      broadcastPresence();
      return;
    }

    // Всё остальное — сообщения с адресатом (`to`): просто пересылаем,
    // как есть, тому логину, кому адресовано. Отправитель ничего
    // обратно от сервера не получает (клиент сам оптимистично обновляет
    // свой UI перед отправкой).
    if (msg.to) {
      sendTo(msg.to, msg);
    }
  });

  ws.on('close', () => {
    unregisterLogin(ws);
    broadcastPresence();
  });

  ws.on('error', () => {});
});

// Пинг раз в 30 секунд — чтобы вовремя убирать «мёртвые» сокеты
// (например, если клиент завис или сеть пропала без нормального close).
const HEARTBEAT_MS = 30000;
const heartbeat = setInterval(() => {
  for (const sockets of clientsByLogin.values()) {
    for (const sock of Array.from(sockets)) {
      if (sock.isAlive === false) {
        sock.terminate();
        continue;
      }
      sock.isAlive = false;
      try { sock.ping(); } catch (e) {}
    }
  }
}, HEARTBEAT_MS);

wss.on('close', () => clearInterval(heartbeat));

console.log(`Nexo WS relay слушает на порту ${PORT} (ws://localhost:${PORT})`);
