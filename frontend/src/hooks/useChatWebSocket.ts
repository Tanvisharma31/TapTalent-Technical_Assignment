import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ChatLine, ConnectionUiStatus, ServerPayload } from '../chatTypes';

const WS_URL = import.meta.env.VITE_WS_URL as string | undefined;

function lineId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

export function useChatWebSocket() {
  const socketRef = useRef<WebSocket | null>(null);
  const sessionIdRef = useRef<string | null>(null);
  const [uiStatus, setUiStatus] = useState<ConnectionUiStatus>('disconnected');
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [lines, setLines] = useState<ChatLine[]>([]);
  const [draft, setDraft] = useState('');
  const [lastNotice, setLastNotice] = useState<string | null>(null);

  const appendSystemLine = useCallback((text: string) => {
    setLines((prev) => [...prev, { id: lineId(), kind: 'system', text }]);
  }, []);

  const sendPayload = useCallback((payload: object) => {
    const socket = socketRef.current;
    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify(payload));
    }
  }, []);

  useEffect(() => {
    if (!WS_URL) {
      setUiStatus('disconnected');
      return;
    }

    setUiStatus('connecting');
    const socket = new WebSocket(WS_URL);
    socketRef.current = socket;

    socket.onopen = () => {
      setUiStatus('idle');
      setLastNotice(null);
      socket.send(JSON.stringify({ action: 'init' }));
    };

    socket.onmessage = (event) => {
      let payload: ServerPayload;
      try {
        payload = JSON.parse(event.data as string) as ServerPayload;
      } catch {
        return;
      }
      const messageType = payload.type;
      if (messageType === 'session' && payload.sessionId) {
        sessionIdRef.current = payload.sessionId;
        setSessionId(payload.sessionId);
        return;
      }
      if (messageType === 'status' && payload.status === 'searching') {
        setUiStatus('searching');
        appendSystemLine('Searching for someone…');
        return;
      }
      if (messageType === 'matched') {
        setUiStatus('in_chat');
        appendSystemLine('Connected. Say hi.');
        return;
      }
      if (messageType === 'message' && typeof payload.text === 'string') {
        const mine = payload.fromSessionId === sessionIdRef.current;
        setLines((prev) => [
          ...prev,
          { id: lineId(), kind: 'msg', text: payload.text!, mine },
        ]);
        return;
      }
      if (messageType === 'chat_ended') {
        setUiStatus('idle');
        const reason = payload.reason ?? 'ended';
        appendSystemLine(`Chat ended (${reason}).`);
        return;
      }
      if (messageType === 'partner_disconnected') {
        setUiStatus('idle');
        setLastNotice(payload.message ?? 'Partner disconnected.');
        appendSystemLine('Partner disconnected.');
        return;
      }
      if (messageType === 'error' && payload.message) {
        appendSystemLine(`Error: ${payload.message}`);
      }
    };

    socket.onclose = () => {
      socketRef.current = null;
      setUiStatus('disconnected');
      appendSystemLine('Disconnected from server.');
    };

    socket.onerror = () => {
      setLastNotice('WebSocket error. Check VITE_WS_URL and deploy.');
    };

    return () => {
      socket.close();
      socketRef.current = null;
    };
  }, [appendSystemLine]);

  const findChat = useCallback(() => {
    setLastNotice(null);
    sendPayload({ action: 'search' });
  }, [sendPayload]);

  const sendMessage = useCallback(() => {
    const text = draft.trim();
    if (!text || uiStatus !== 'in_chat') return;
    sendPayload({ action: 'message', text });
    setLines((prev) => [...prev, { id: lineId(), kind: 'msg', text, mine: true }]);
    setDraft('');
  }, [draft, sendPayload, uiStatus]);

  const skipChat = useCallback(() => {
    sendPayload({ action: 'skip' });
  }, [sendPayload]);

  const statusLabel = useMemo(() => {
    switch (uiStatus) {
      case 'disconnected':
        return 'Disconnected';
      case 'connecting':
        return 'Connecting…';
      case 'searching':
        return 'Searching';
      case 'in_chat':
        return 'Connected';
      default:
        return 'Idle';
    }
  }, [uiStatus]);

  return {
    wsUrlConfigured: Boolean(WS_URL),
    uiStatus,
    sessionId,
    lines,
    draft,
    setDraft,
    lastNotice,
    statusLabel,
    findChat,
    sendMessage,
    skipChat,
  };
}
