import type { APIGatewayProxyWebsocketHandlerV2 } from 'aws-lambda';
import { ApiGatewayManagementApiClient, PostToConnectionCommand } from '@aws-sdk/client-apigatewaymanagementapi';
import { randomUUID } from 'crypto';
import { col, colNum, executeSql, withTransaction } from './db';

const MAX_MESSAGE_LENGTH = 2000;
const RATE_WINDOW_MS = 60_000;
const RATE_MAX_MESSAGES = 30;

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
];

async function ensureSchema(): Promise<void> {
  if (schemaReady) return;
  for (const sql of DDL) {
    await executeSql(sql);
  }
  schemaReady = true;
}

function mgmtClient(domainName: string, stage: string): ApiGatewayManagementApiClient {
  return new ApiGatewayManagementApiClient({
    endpoint: `https://${domainName}/${stage}`,
  });
}

async function sendJson(
  client: ApiGatewayManagementApiClient,
  connectionId: string,
  payload: unknown,
): Promise<void> {
  const data = Buffer.from(JSON.stringify(payload), 'utf8');
  try {
    await client.send(
      new PostToConnectionCommand({ ConnectionId: connectionId, Data: data }),
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
  const client = mgmtClient(domainName, stage);
  const partner = await withTransaction(async (tx) => {
    const p = await executeSql(
      `SELECT partner_id FROM connections WHERE connection_id = $1 FOR UPDATE`,
      [connectionId],
      tx,
    );
    const partnerId = col(p.rows, 'partner_id');
    if (partnerId) {
      await executeSql(
        `UPDATE connections SET partner_id = NULL, status = 'idle', chat_id = NULL, updated_at = NOW()
         WHERE connection_id = $1`,
        [partnerId],
        tx,
      );
    }
    await executeSql(`DELETE FROM rate_limits WHERE connection_id = $1`, [connectionId], tx);
    await executeSql(`DELETE FROM connections WHERE connection_id = $1`, [connectionId], tx);
    return partnerId;
  });
  if (partner) {
    await sendJson(client, partner, {
      type: 'partner_disconnected',
      message: 'Your chat partner disconnected.',
    });
  }
}

async function handleInit(connectionId: string, domainName: string, stage: string): Promise<void> {
  await ensureSchema();
  const client = mgmtClient(domainName, stage);
  const r = await executeSql(`SELECT session_id FROM connections WHERE connection_id = $1`, [
    connectionId,
  ]);
  let sessionId = col(r.rows, 'session_id');
  if (!sessionId) {
    await handleConnect(connectionId);
    const r2 = await executeSql(`SELECT session_id FROM connections WHERE connection_id = $1`, [
      connectionId,
    ]);
    sessionId = col(r2.rows, 'session_id');
  }
  await sendJson(client, connectionId, { type: 'session', sessionId });
}

async function handleSearch(connectionId: string, domainName: string, stage: string): Promise<void> {
  await ensureSchema();
  const client = mgmtClient(domainName, stage);

  const match = await withTransaction(async (tx) => {
    let notifyPartnerId: string | undefined;
    const selfRow = await executeSql(
      `SELECT status, partner_id FROM connections WHERE connection_id = $1 FOR UPDATE`,
      [connectionId],
      tx,
    );
    const selfStatus = col(selfRow.rows, 'status');
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
      const partnerId = col(selfRow.rows, 'partner_id');
      if (partnerId) {
        notifyPartnerId = partnerId;
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

    const partnerPick = await executeSql(
      `SELECT connection_id, session_id FROM connections
       WHERE status = 'waiting' AND connection_id <> $1
       ORDER BY updated_at ASC
       LIMIT 1
       FOR UPDATE SKIP LOCKED`,
      [connectionId],
      tx,
    );
    const partnerConn = col(partnerPick.rows, 'connection_id');
    const partnerSession = col(partnerPick.rows, 'session_id');
    if (!partnerConn) {
      return {
        partnerConn: undefined as string | undefined,
        partnerSession: undefined as string | undefined,
        mySession: undefined as string | undefined,
        chatId: undefined as string | undefined,
        notifyPartnerId,
      };
    }

    const selfSess = await executeSql(
      `SELECT session_id FROM connections WHERE connection_id = $1`,
      [connectionId],
      tx,
    );
    const mySession = col(selfSess.rows, 'session_id');
    const chatId = randomUUID();

    await executeSql(
      `UPDATE connections SET status = 'in_chat', partner_id = $2, chat_id = $3, updated_at = NOW()
       WHERE connection_id = $1`,
      [connectionId, partnerConn, chatId],
      tx,
    );
    await executeSql(
      `UPDATE connections SET status = 'in_chat', partner_id = $2, chat_id = $3, updated_at = NOW()
       WHERE connection_id = $1`,
      [partnerConn, connectionId, chatId],
      tx,
    );

    return {
      partnerConn,
      partnerSession,
      mySession,
      chatId,
      notifyPartnerId,
    };
  });

  if (match?.notifyPartnerId) {
    await sendJson(client, match.notifyPartnerId, {
      type: 'chat_ended',
      reason: 'partner_rematching',
    });
  }

  if (!match?.partnerConn || !match.mySession) {
    await sendJson(client, connectionId, { type: 'status', status: 'searching' });
    return;
  }

  await sendJson(client, connectionId, {
    type: 'matched',
    chatId: match.chatId,
    partnerSessionId: match.partnerSession,
    yourSessionId: match.mySession,
  });
  await sendJson(client, match.partnerConn, {
    type: 'matched',
    chatId: match.chatId,
    partnerSessionId: match.mySession,
    yourSessionId: match.partnerSession,
  });
}

async function checkRateLimit(connectionId: string): Promise<boolean> {
  const now = Date.now();
  const r = await executeSql(`SELECT window_start_ms, msg_count FROM rate_limits WHERE connection_id = $1`, [
    connectionId,
  ]);
  const start = colNum(r.rows, 'window_start_ms');
  const count = colNum(r.rows, 'msg_count');
  if (start === undefined) {
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
  if (now - start > RATE_WINDOW_MS) {
    await executeSql(`UPDATE rate_limits SET window_start_ms = $1, msg_count = 1 WHERE connection_id = $2`, [
      now,
      connectionId,
    ]);
    return true;
  }
  if ((count ?? 0) >= RATE_MAX_MESSAGES) {
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
  const client = mgmtClient(domainName, stage);
  if (text.length > MAX_MESSAGE_LENGTH) {
    await sendJson(client, connectionId, {
      type: 'error',
      message: `Message too long (max ${MAX_MESSAGE_LENGTH} characters).`,
    });
    return;
  }
  const ok = await checkRateLimit(connectionId);
  if (!ok) {
    await sendJson(client, connectionId, {
      type: 'error',
      message: `Rate limit: at most ${RATE_MAX_MESSAGES} messages per ${RATE_WINDOW_MS / 1000} seconds.`,
    });
    return;
  }

  const row = await executeSql(
    `SELECT partner_id, session_id, status FROM connections WHERE connection_id = $1`,
    [connectionId],
  );
  const partnerId = col(row.rows, 'partner_id');
  const mySession = col(row.rows, 'session_id');
  const status = col(row.rows, 'status');
  if (status !== 'in_chat' || !partnerId) {
    await sendJson(client, connectionId, {
      type: 'error',
      message: 'Not in an active chat.',
    });
    return;
  }

  await sendJson(client, partnerId, {
    type: 'message',
    text,
    fromSessionId: mySession,
  });
}

async function handleSkip(connectionId: string, domainName: string, stage: string): Promise<void> {
  await ensureSchema();
  const client = mgmtClient(domainName, stage);
  const partnerId = await withTransaction(async (tx) => {
    const r = await executeSql(
      `SELECT partner_id, status FROM connections WHERE connection_id = $1 FOR UPDATE`,
      [connectionId],
      tx,
    );
    const pid = col(r.rows, 'partner_id');
    const st = col(r.rows, 'status');
    if (st !== 'in_chat' || !pid) {
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
      [connectionId, pid],
      tx,
    );
    return pid;
  });

  await sendJson(client, connectionId, { type: 'chat_ended', reason: 'you_skipped' });
  if (partnerId) {
    await sendJson(client, partnerId, {
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
      await handleConnect(connectionId);
      return { statusCode: 200 };
    }
    if (routeKey === '$disconnect') {
      await handleDisconnect(connectionId, domainName, stage);
      return { statusCode: 200 };
    }

    let body: { action?: string; text?: string } = {};
    if (event.body) {
      try {
        body = JSON.parse(event.body) as { action?: string; text?: string };
      } catch {
        await sendJson(mgmtClient(domainName, stage), connectionId, {
          type: 'error',
          message: 'Invalid JSON body.',
        });
        return { statusCode: 200 };
      }
    }

    const rk = routeKey;
    const action =
      rk === '$default'
        ? typeof body.action === 'string'
          ? body.action
          : 'message'
        : rk;
    if (action === 'init') {
      await handleInit(connectionId, domainName, stage);
    } else if (action === 'search') {
      await handleSearch(connectionId, domainName, stage);
    } else if (action === 'skip' || action === 'end') {
      await handleSkip(connectionId, domainName, stage);
    } else if (action === 'message') {
      const text = typeof body.text === 'string' ? body.text : '';
      if (!text.trim()) {
        await sendJson(mgmtClient(domainName, stage), connectionId, {
          type: 'error',
          message: 'Empty message.',
        });
      } else {
        await handleMessage(connectionId, text, domainName, stage);
      }
    } else {
      await sendJson(mgmtClient(domainName, stage), connectionId, {
        type: 'error',
        message: `Unknown action: ${action}`,
      });
    }

    return { statusCode: 200 };
  } catch (err) {
    console.error(err);
    try {
      await sendJson(mgmtClient(domainName, stage), connectionId, {
        type: 'error',
        message: 'Internal error.',
      });
    } catch {
      /* ignore */
    }
    return { statusCode: 200 };
  }
};
