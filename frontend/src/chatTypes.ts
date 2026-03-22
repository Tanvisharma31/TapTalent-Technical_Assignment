export type ConnectionUiStatus =
  | 'disconnected'
  | 'connecting'
  | 'idle'
  | 'searching'
  | 'in_chat';

export type ChatLine =
  | { id: string; kind: 'system'; text: string }
  | { id: string; kind: 'msg'; text: string; mine: boolean };

export type ServerPayload = {
  type?: string;
  status?: string;
  sessionId?: string;
  chatId?: string;
  yourSessionId?: string;
  partnerSessionId?: string;
  text?: string;
  fromSessionId?: string;
  reason?: string;
  message?: string;
};
