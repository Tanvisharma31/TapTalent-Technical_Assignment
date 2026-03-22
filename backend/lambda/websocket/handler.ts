import type { APIGatewayProxyWebsocketHandlerV2 } from 'aws-lambda';
import { ApiGatewayManagementApiClient, PostToConnectionCommand } from '@aws-sdk/client-apigatewaymanagementapi';
import { randomUUID } from 'crypto';
import { executeSql, scalarNumber, scalarString, withTransaction } from './db';

const MAX_MESSAGE_LENGTH = 2000;
const RATE_WINDOW_MS = 60_000;
const RATE_MAX_MESSAGES = 30;

/** Max WebSocket route invocations per connection per window (search/skip/init/message, etc.). */
const ROUTE_RATE_WINDOW_MS = 60_000;
const ROUTE_RATE_MAX_EVENTS = 45;

/** Reject new $connect when this many rows exist (limits DB + Lambda blast radius). */
const MAX_GLOBAL_CONNECTIONS = 200;

/** New $connect attempts per client IP per window (reduces bot / scripted abuse). */
const CONNECT_IP_WINDOW_MS = 900_000;
const CONNECT_IP_MAX = 30;

let schemaReady = false;

const DDL = [
  `CREATE TABLE IF NOT EXISTS connections (
    connection_id VARCHAR(128) PRIMARY KEY,
    session_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    partner_id VARCHAR(128),
    chat_id VARCHAR(128),
    updated_at TIMESTAMPTZ DEFAULT NOW()
  )`,
  `CREATE INDEX IF NOT EXISTS idx_connections_waiting ON connections (status, updated_at)
   WHERE status = 'waiting'`,
  `CREATE TABLE IF NOT EXISTS rate_limits (
    connection_id VARCHAR(128) PRIMARY KEY,
    window_start_ms BIGINT NOT NULL,
    msg_count INT NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS route_rate_limits (
    connection_id VARCHAR(128) PRIMARY KEY,
    window_start_ms BIGINT NOT NULL,
    event_count INT NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS connect_ip_limits (
    ip_address VARCHAR(64) PRIMARY KEY,
    window_start_ms BIGINT NOT NULL,
    connect_count INT NOT NULL
  )`,
];

async function ensureSchema(): Promise<void> {
  if (schemaReady) return;
  for (const sql of DDL) {
    await executeSql(sql);
  }
  schemaReady = true;
}

function managementClient(domainName: string, stage: string): ApiGatewayManagementApiClient {
  return new ApiGatewayManagementApiClient({
    endpoint: `https://${domainName}/${stage}`,
  });
}

async function postJsonToConnection(
  client: ApiGatewayManagementApiClient,
  connectionId: string,
  payload: unknown,
): Promise<void> {
  const bytes = Buffer.from(JSON.stringify(payload), 'utf8');
  try {
    await client.send(
      new PostToConnectionCommand({ ConnectionId: connectionId, Data: bytes }),
    );
  } catch (e) {
    if (typeof e === 'object' && e !== null && (e as { name?: string }).name === 'GoneException') {
      return;
    }
    throw e;
  }
}

async function handleConnect(connectionId: string): Promise<void> {
  await ensureSchema();
  const sessionId = randomUUID();
  await executeSql(
    `INSERT INTO connections (connection_id, session_id, status)
     VALUES ($1, $2, 'idle')
     ON CONFLICT (connection_id) DO UPDATE SET
       session_id = EXCLUDED.session_id,
       status = 'idle',
       partner_id = NULL,
       chat_id = NULL,
       updated_at = NOW()`,
    [connectionId, sessionId],
  );
}

async function handleDisconnect(connectionId: string, domainName: string, stage: string): Promise<void> {
  await ensureSchema();
  const client = managementClient(domainName, stage);
  const partnerConnectionId = await withTransaction(async (tx) => {
    const partnerLookup = await executeSql(
      `SELECT partner_id FROM connections WHERE connection_id = $1 FOR UPDATE`,
      [connectionId],
      tx,
    );
    const partnerId = scalarString(partnerLookup.rows, 'partner_id');
    if (partnerId) {
      await executeSql(
        `UPDATE connections SET partner_id = NULL, status = 'idle', chat_id = NULL, updated_at = NOW()
         WHERE connection_id = $1`,
        [partnerId],
        tx,
      );
    }
    await executeSql(`DELETE FROM rate_limits WHERE connection_id = $1`, [connectionId], tx);
    await executeSql(`DELETE FROM route_rate_limits WHERE connection_id = $1`, [connectionId], tx);
    await executeSql(`DELETE FROM connections WHERE connection_id = $1`, [connectionId], tx);
    return partnerId;
  });
  if (partnerConnectionId) {
    try {
      await postJsonToConnection(client, partnerConnectionId, {
        type: 'partner_disconnected',
        message: 'Your chat partner disconnected.',
      });
    } catch (err) {
      console.warn('[ws] notify partner_disconnected failed', {
        droppedConnectionId: connectionId,
        partnerConnectionId,
        err: err instanceof Error ? err.message : err,
      });
    }
  }
}

async function handleInit(connectionId: string, domainName: string, stage: string): Promise<void> {
  await ensureSchema();
  const client = managementClient(domainName, stage);
  const sessionRow = await executeSql(`SELECT session_id FROM connections WHERE connection_id = $1`, [
    connectionId,
  ]);
  let sessionId = scalarString(sessionRow.rows, 'session_id');
  if (!sessionId) {
    await handleConnect(connectionId);
    const afterConnect = await executeSql(`SELECT session_id FROM connections WHERE connection_id = $1`, [
      connectionId,
    ]);
    sessionId = scalarString(afterConnect.rows, 'session_id');
  }
  await postJsonToConnection(client, connectionId, { type: 'session', sessionId });
}

async function handleSearch(connectionId: string, domainName: string, stage: string): Promise<void> {
  await ensureSchema();
  const client = managementClient(domainName, stage);

  const match = await withTransaction(async (tx) => {
    let previousPartnerToNotify: string | undefined;
    const selfRow = await executeSql(
      `SELECT status, partner_id FROM connections WHERE connection_id = $1 FOR UPDATE`,
      [connectionId],
      tx,
    );
    const selfStatus = scalarString(selfRow.rows, 'status');
    if (!selfStatus) {
      await executeSql(
        `INSERT INTO connections (connection_id, session_id, status)
         VALUES ($1, $2, 'waiting')
         ON CONFLICT (connection_id) DO UPDATE SET
           status = 'waiting',
           partner_id = NULL,
           chat_id = NULL,
           updated_at = NOW()`,
        [connectionId, randomUUID()],
        tx,
      );
    } else if (selfStatus === 'in_chat') {
      const partnerId = scalarString(selfRow.rows, 'partner_id');
      if (partnerId) {
        previousPartnerToNotify = partnerId;
        await executeSql(
          `UPDATE connections SET partner_id = NULL, status = 'idle', chat_id = NULL, updated_at = NOW()
           WHERE connection_id = $1`,
          [partnerId],
          tx,
        );
      }
      await executeSql(
        `UPDATE connections SET status = 'waiting', partner_id = NULL, chat_id = NULL, updated_at = NOW()
         WHERE connection_id = $1`,
        [connectionId],
        tx,
      );
    } else {
      await executeSql(
        `UPDATE connections SET status = 'waiting', partner_id = NULL, chat_id = NULL, updated_at = NOW()
         WHERE connection_id = $1`,
        [connectionId],
        tx,
      );
    }

    const oldestWaiter = await executeSql(
      `SELECT connection_id, session_id FROM connections
       WHERE status = 'waiting' AND connection_id <> $1
       ORDER BY updated_at ASC
       LIMIT 1
       FOR UPDATE SKIP LOCKED`,
      [connectionId],
      tx,
    );
    const peerConnectionId = scalarString(oldestWaiter.rows, 'connection_id');
    const peerSessionId = scalarString(oldestWaiter.rows, 'session_id');
    if (!peerConnectionId) {
      return {
        peerConnectionId: undefined as string | undefined,
        peerSessionId: undefined as string | undefined,
        mySessionId: undefined as string | undefined,
        chatId: undefined as string | undefined,
        previousPartnerToNotify,
      };
    }

    const mySessionRow = await executeSql(
      `SELECT session_id FROM connections WHERE connection_id = $1`,
      [connectionId],
      tx,
    );
    const mySessionId = scalarString(mySessionRow.rows, 'session_id');
    const chatId = randomUUID();

    await executeSql(
      `UPDATE connections SET status = 'in_chat', partner_id = $2, chat_id = $3, updated_at = NOW()
       WHERE connection_id = $1`,
      [connectionId, peerConnectionId, chatId],
      tx,
    );
    await executeSql(
      `UPDATE connections SET status = 'in_chat', partner_id = $2, chat_id = $3, updated_at = NOW()
       WHERE connection_id = $1`,
      [peerConnectionId, connectionId, chatId],
      tx,
    );

    return {
      peerConnectionId,
      peerSessionId,
      mySessionId,
      chatId,
      previousPartnerToNotify,
    };
  });

  if (match?.previousPartnerToNotify) {
    await postJsonToConnection(client, match.previousPartnerToNotify, {
      type: 'chat_ended',
      reason: 'partner_rematching',
    });
  }

  if (!match?.peerConnectionId || !match.mySessionId) {
    await postJsonToConnection(client, connectionId, { type: 'status', status: 'searching' });
    return;
  }

  await postJsonToConnection(client, connectionId, {
    type: 'matched',
    chatId: match.chatId,
    partnerSessionId: match.peerSessionId,
    yourSessionId: match.mySessionId,
  });
  await postJsonToConnection(client, match.peerConnectionId, {
    type: 'matched',
    chatId: match.chatId,
    partnerSessionId: match.mySessionId,
    yourSessionId: match.peerSessionId,
  });
}

async function checkRouteRateLimit(connectionId: string): Promise<boolean> {
  const now = Date.now();
  const r = await executeSql(
    `SELECT window_start_ms, event_count FROM route_rate_limits WHERE connection_id = $1`,
    [connectionId],
  );
  const start = scalarNumber(r.rows, 'window_start_ms');
  const count = scalarNumber(r.rows, 'event_count');
  if (start === undefined) {
    await executeSql(
      `INSERT INTO route_rate_limits (connection_id, window_start_ms, event_count)
       VALUES ($1, $2, 1)
       ON CONFLICT (connection_id) DO UPDATE SET
         window_start_ms = EXCLUDED.window_start_ms,
         event_count = EXCLUDED.event_count`,
      [connectionId, now],
    );
    return true;
  }
  if (now - start > ROUTE_RATE_WINDOW_MS) {
    await executeSql(
      `UPDATE route_rate_limits SET window_start_ms = $1, event_count = 1 WHERE connection_id = $2`,
      [now, connectionId],
    );
    return true;
  }
  if ((count ?? 0) >= ROUTE_RATE_MAX_EVENTS) {
    return false;
  }
  await executeSql(`UPDATE route_rate_limits SET event_count = event_count + 1 WHERE connection_id = $1`, [
    connectionId,
  ]);
  return true;
}

async function checkIpConnectLimit(ip: string): Promise<boolean> {
  const now = Date.now();
  const r = await executeSql(
    `SELECT window_start_ms, connect_count FROM connect_ip_limits WHERE ip_address = $1`,
    [ip],
  );
  const start = scalarNumber(r.rows, 'window_start_ms');
  const count = scalarNumber(r.rows, 'connect_count');
  if (start === undefined) {
    await executeSql(
      `INSERT INTO connect_ip_limits (ip_address, window_start_ms, connect_count)
       VALUES ($1, $2, 1)
       ON CONFLICT (ip_address) DO UPDATE SET
         window_start_ms = EXCLUDED.window_start_ms,
         connect_count = EXCLUDED.connect_count`,
      [ip, now],
    );
    return true;
  }
  if (now - start > CONNECT_IP_WINDOW_MS) {
    await executeSql(
      `UPDATE connect_ip_limits SET window_start_ms = $1, connect_count = 1 WHERE ip_address = $2`,
      [now, ip],
    );
    return true;
  }
  if ((count ?? 0) >= CONNECT_IP_MAX) {
    return false;
  }
  await executeSql(`UPDATE connect_ip_limits SET connect_count = connect_count + 1 WHERE ip_address = $1`, [
    ip,
  ]);
  return true;
}

async function checkRateLimit(connectionId: string): Promise<boolean> {
  const now = Date.now();
  const limitRow = await executeSql(
    `SELECT window_start_ms, msg_count FROM rate_limits WHERE connection_id = $1`,
    [connectionId],
  );
  const windowStartMs = scalarNumber(limitRow.rows, 'window_start_ms');
  const messageCount = scalarNumber(limitRow.rows, 'msg_count');
  if (windowStartMs === undefined) {
    await executeSql(
      `INSERT INTO rate_limits (connection_id, window_start_ms, msg_count)
       VALUES ($1, $2, 1)
       ON CONFLICT (connection_id) DO UPDATE SET
         window_start_ms = EXCLUDED.window_start_ms,
         msg_count = EXCLUDED.msg_count`,
      [connectionId, now],
    );
    return true;
  }
  if (now - windowStartMs > RATE_WINDOW_MS) {
    await executeSql(`UPDATE rate_limits SET window_start_ms = $1, msg_count = 1 WHERE connection_id = $2`, [
      now,
      connectionId,
    ]);
    return true;
  }
  if ((messageCount ?? 0) >= RATE_MAX_MESSAGES) {
    return false;
  }
  await executeSql(`UPDATE rate_limits SET msg_count = msg_count + 1 WHERE connection_id = $1`, [
    connectionId,
  ]);
  return true;
}

async function handleMessage(
  connectionId: string,
  text: string,
  domainName: string,
  stage: string,
): Promise<void> {
  await ensureSchema();
  const client = managementClient(domainName, stage);
  if (text.length > MAX_MESSAGE_LENGTH) {
    await postJsonToConnection(client, connectionId, {
      type: 'error',
      message: `Message too long (max ${MAX_MESSAGE_LENGTH} characters).`,
    });
    return;
  }
  const withinLimit = await checkRateLimit(connectionId);
  if (!withinLimit) {
    await postJsonToConnection(client, connectionId, {
      type: 'error',
      message: `Rate limit: at most ${RATE_MAX_MESSAGES} messages per ${RATE_WINDOW_MS / 1000} seconds.`,
    });
    return;
  }

  const chatRow = await executeSql(
    `SELECT partner_id, session_id, status FROM connections WHERE connection_id = $1`,
    [connectionId],
  );
  const partnerId = scalarString(chatRow.rows, 'partner_id');
  const mySessionId = scalarString(chatRow.rows, 'session_id');
  const status = scalarString(chatRow.rows, 'status');
  if (status !== 'in_chat' || !partnerId) {
    await postJsonToConnection(client, connectionId, {
      type: 'error',
      message: 'Not in an active chat.',
    });
    return;
  }

  await postJsonToConnection(client, partnerId, {
    type: 'message',
    text,
    fromSessionId: mySessionId,
  });
}

async function handleSkip(connectionId: string, domainName: string, stage: string): Promise<void> {
  await ensureSchema();
  const client = managementClient(domainName, stage);
  const partnerId = await withTransaction(async (tx) => {
    const chatState = await executeSql(
      `SELECT partner_id, status FROM connections WHERE connection_id = $1 FOR UPDATE`,
      [connectionId],
      tx,
    );
    const peerId = scalarString(chatState.rows, 'partner_id');
    const status = scalarString(chatState.rows, 'status');
    if (status !== 'in_chat' || !peerId) {
      await executeSql(
        `UPDATE connections SET status = 'idle', partner_id = NULL, chat_id = NULL, updated_at = NOW()
         WHERE connection_id = $1`,
        [connectionId],
        tx,
      );
      return null;
    }
    await executeSql(
      `UPDATE connections SET partner_id = NULL, status = 'idle', chat_id = NULL, updated_at = NOW()
       WHERE connection_id = $1 OR connection_id = $2`,
      [connectionId, peerId],
      tx,
    );
    return peerId;
  });

  await postJsonToConnection(client, connectionId, { type: 'chat_ended', reason: 'you_skipped' });
  if (partnerId) {
    await postJsonToConnection(client, partnerId, {
      type: 'chat_ended',
      reason: 'partner_skipped',
    });
  }
}

export const handler: APIGatewayProxyWebsocketHandlerV2 = async (event) => {
  const { routeKey, connectionId, domainName, stage } = event.requestContext;
  if (!connectionId || !domainName || !stage) {
    return { statusCode: 400 };
  }

  try {
    if (routeKey === '$connect') {
      await ensureSchema();
      const cnt = await executeSql(`SELECT COUNT(*)::int AS c FROM connections`);
      const open = scalarNumber(cnt.rows, 'c') ?? 0;
      if (open >= MAX_GLOBAL_CONNECTIONS) {
        return { statusCode: 429, body: 'Server at capacity' };
      }
      const ip = (event.requestContext as { identity?: { sourceIp?: string } }).identity?.sourceIp;
      if (ip) {
        const okIp = await checkIpConnectLimit(ip);
        if (!okIp) {
          return { statusCode: 429, body: 'Too many connections from this network' };
        }
      }
      await handleConnect(connectionId);
      return { statusCode: 200 };
    }
    if (routeKey === '$disconnect') {
      await handleDisconnect(connectionId, domainName, stage);
      return { statusCode: 200 };
    }

    await ensureSchema();
    const routeOk = await checkRouteRateLimit(connectionId);
    if (!routeOk) {
      await postJsonToConnection(managementClient(domainName, stage), connectionId, {
        type: 'error',
        message: `Too many requests. At most ${ROUTE_RATE_MAX_EVENTS} events per ${ROUTE_RATE_WINDOW_MS / 1000}s per connection.`,
      });
      return { statusCode: 200 };
    }

    let body: { action?: string; text?: string } = {};
    if (event.body) {
      try {
        body = JSON.parse(event.body) as { action?: string; text?: string };
      } catch {
        await postJsonToConnection(managementClient(domainName, stage), connectionId, {
          type: 'error',
          message: 'Invalid JSON body.',
        });
        return { statusCode: 200 };
      }
    }

    const action =
      routeKey === '$default'
        ? typeof body.action === 'string'
          ? body.action
          : 'message'
        : routeKey;
    if (action === 'init') {
      await handleInit(connectionId, domainName, stage);
    } else if (action === 'search') {
      await handleSearch(connectionId, domainName, stage);
    } else if (action === 'skip' || action === 'end') {
      await handleSkip(connectionId, domainName, stage);
    } else if (action === 'message') {
      const text = typeof body.text === 'string' ? body.text : '';
      if (!text.trim()) {
        await postJsonToConnection(managementClient(domainName, stage), connectionId, {
          type: 'error',
          message: 'Empty message.',
        });
      } else {
        await handleMessage(connectionId, text, domainName, stage);
      }
    } else {
      await postJsonToConnection(managementClient(domainName, stage), connectionId, {
        type: 'error',
        message: `Unknown action: ${action}`,
      });
    }

    return { statusCode: 200 };
  } catch (err) {
    console.error('[ws] unhandled', { routeKey, connectionId, err });
    try {
      await postJsonToConnection(managementClient(domainName, stage), connectionId, {
        type: 'error',
        message: 'Internal error.',
      });
    } catch {
      // Client likely gone; nothing to do.
    }
    return { statusCode: 200 };
  }
};
