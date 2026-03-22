import './App.css';
import { ChatMessages } from './components/ChatMessages';
import { useChatWebSocket } from './hooks/useChatWebSocket';

export default function App() {
  const {
    wsUrlConfigured,
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
  } = useChatWebSocket();

  if (!wsUrlConfigured) {
    return (
      <div className="app">
        <h1>TapTalent chat</h1>
        <p className="warn">
          Set <code>VITE_WS_URL</code> to your API Gateway WebSocket URL (for example{' '}
          <code>wss://xxxxx.execute-api.us-east-1.amazonaws.com/prod</code>), then rebuild.
        </p>
      </div>
    );
  }

  const findDisabled =
    uiStatus === 'disconnected' ||
    uiStatus === 'connecting' ||
    uiStatus === 'searching';

  return (
    <div className="app">
      <header className="header">
        <h1>TapTalent anonymous chat</h1>
        <div className="meta">
          <span className={`pill pill-${uiStatus}`} title="Connection state">
            {statusLabel}
          </span>
          {sessionId && (
            <span className="session" title="Your session id (opaque to others)">
              You: {sessionId.slice(0, 8)}…
            </span>
          )}
        </div>
        {lastNotice && <p className="notice">{lastNotice}</p>}
      </header>

      <ChatMessages lines={lines} />

      <div className="composer">
        <div className="row">
          <input
            type="text"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && sendMessage()}
            placeholder={
              uiStatus === 'in_chat' ? 'Message…' : 'Match first to send messages'
            }
            disabled={uiStatus !== 'in_chat'}
            maxLength={2000}
            aria-label="Chat message"
          />
          <button
            type="button"
            onClick={sendMessage}
            disabled={uiStatus !== 'in_chat' || !draft.trim()}
          >
            Send
          </button>
        </div>
        <div className="actions">
          <button type="button" onClick={findChat} disabled={findDisabled}>
            Find chat
          </button>
          <button type="button" onClick={skipChat} disabled={uiStatus !== 'in_chat'}>
            Skip / end
          </button>
        </div>
      </div>
    </div>
  );
}
