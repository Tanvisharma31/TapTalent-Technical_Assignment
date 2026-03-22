import { memo, useEffect, useRef } from 'react';
import type { ChatLine } from '../chatTypes';

type Props = {
  lines: ChatLine[];
};

function ChatMessagesInner({ lines }: Props) {
  const endRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [lines]);

  return (
    <div className="messages" role="log" aria-live="polite">
      {lines.length === 0 && (
        <p className="empty">Open a second tab to test matching.</p>
      )}
      {lines.map((line) =>
        line.kind === 'system' ? (
          <div key={line.id} className="line system">
            {line.text}
          </div>
        ) : (
          <div
            key={line.id}
            className={`line msg ${line.mine ? 'mine' : 'theirs'}`}
          >
            {line.text}
          </div>
        ),
      )}
      <div ref={endRef} aria-hidden />
    </div>
  );
}

export const ChatMessages = memo(ChatMessagesInner);
