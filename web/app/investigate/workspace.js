"use client";

import { useEffect, useMemo, useState } from "react";

const API = process.env.NEXT_PUBLIC_DEMO_API || "http://127.0.0.1:8787";

export function Workspace() {
  const [sessionId, setSessionId] = useState("");
  const [messages, setMessages] = useState([]);
  const [draft, setDraft] = useState("");
  const [busy, setBusy] = useState(false);
  const [graph, setGraph] = useState(null);
  const [book, setBook] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [selected, setSelected] = useState("");
  const [suggestions, setSuggestions] = useState([]);

  useEffect(() => {
    if (!graph) {
      setSelected("");
      return;
    }
    setSelected((current) =>
      graph.edges.some((edge) => edge.id === current) ? current : String(graph.focal),
    );
  }, [graph]);

  const selectedEdge = graph?.edges.find((edge) => edge.id === selected) || null;

  function send(event) {
    event.preventDefault();
    deliver(draft);
  }

  async function deliver(text) {
    const message = text.trim();
    if (!message || busy) return;
    setDraft("");
    setSuggestions([]);
    setBusy(true);
    setMessages((current) => [
      ...current,
      { role: "operator", text: message },
      { role: "agent", text: "" },
    ]);
    try {
      const response = await fetch(`${API}/demo/messages`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message, session_id: sessionId }),
      });
      if (!response.ok || !response.body) {
        const failed = await response.json().catch(() => ({}));
        replaceAgent(failed.error || "The sample did not answer.");
        return;
      }
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (true) {
        const read = await reader.read();
        if (read.done) break;
        buffer += decoder.decode(read.value, { stream: true });
        const parts = buffer.split("\n\n");
        buffer = parts.pop() || "";
        for (const part of parts) applyEvent(part);
      }
    } catch {
      replaceAgent("The sample is not reachable.");
    } finally {
      setBusy(false);
    }
  }

  function applyEvent(part) {
    const line = part.split("\n").find((item) => item.startsWith("data: "));
    if (!line) return;
    const event = JSON.parse(line.slice(6));
    if (event.type === "session") setSessionId(event.session_id);
    if (event.type === "token") appendToken(event.text);
    if (event.type === "done") {
      replaceAgent(event.answer || "");
      if (event.session_id) setSessionId(event.session_id);
      setGraph(event.graph);
      setBook(Boolean(event.book));
      setSuggestions(Array.isArray(event.prompts) ? event.prompts.filter(Boolean) : []);
    }
    if (event.type === "error") replaceAgent(event.error || "The sample did not answer.");
  }

  function appendToken(text) {
    setMessages((current) => {
      const next = current.slice();
      const last = next[next.length - 1];
      if (last?.role === "agent") next[next.length - 1] = { ...last, text: last.text + text };
      return next;
    });
  }

  function replaceAgent(text) {
    setMessages((current) => {
      const next = current.slice();
      const last = next[next.length - 1];
      if (last?.role === "agent") next[next.length - 1] = { ...last, text };
      return next;
    });
  }

  function reset() {
    setSessionId("");
    setMessages([]);
    setGraph(null);
    setBook(false);
    setExpanded(false);
    setSelected("");
    setSuggestions([]);
  }

  return (
    <div className={graph ? `workspace${expanded ? " wide" : ""}` : "workspace solo"}>
      <section className="conversation">
        <header>
          <p>One sample of payments</p>
          <div>
            {book && sessionId ? (
              <a className="book" href={`${API}/demo/book/${sessionId}`}>
                Download case book
              </a>
            ) : null}
            {messages.length ? (
              <button className="quiet" type="button" onClick={reset} disabled={busy}>
                New conversation
              </button>
            ) : null}
          </div>
        </header>
        <div className="thread">
          {messages.length === 0 ? (
            <>
              <p className="empty-note">
                Start a conversation. It reads this sample only, and it is gone when you leave the page.
              </p>
              <button
                type="button"
                className="bubble starter"
                onClick={() => deliver("List the leads")}
                disabled={busy}
              >
                List the leads
              </button>
            </>
          ) : (
            messages.map((item, index) =>
              item.role === "agent" ? (
                <div key={index} className="bubble agent">
                  <Reply text={item.text} />
                </div>
              ) : (
                <p key={index} className={`bubble ${item.role}`}>
                  {item.text}
                </p>
              ),
            )
          )}
        </div>
        {suggestions.length ? (
          <div className="prompts">
            {suggestions.map((prompt) => (
              <button
                key={prompt}
                type="button"
                className="bubble starter"
                onClick={() => deliver(prompt)}
                disabled={busy}
              >
                {prompt}
              </button>
            ))}
          </div>
        ) : null}
        <form className="composer" onSubmit={send}>
          <input
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            placeholder="Ask about a lead, an account, or a payment"
            aria-label="Message"
            disabled={busy}
          />
          <button type="submit" disabled={busy || !draft.trim()}>
            Send
          </button>
        </form>
      </section>
      {graph ? (
        <GraphColumn
          graph={graph}
          expanded={expanded}
          selected={selected}
          selectedEdge={selectedEdge}
          onExpand={() => setExpanded((open) => !open)}
          onSelect={setSelected}
        />
      ) : null}
    </div>
  );
}

function GraphColumn({ graph, expanded, selected, selectedEdge, onExpand, onSelect }) {
  const placed = useMemo(() => place(graph), [graph]);
  return (
    <aside className="graph-column">
      <header>
        <p>Payments from this reading</p>
        <button type="button" onClick={onExpand}>
          {expanded ? "Close" : "Expand"}
        </button>
      </header>
      <div className="reading">
        <div className="canvas">
          <svg width={placed.width} height={placed.height} aria-hidden="true">
            {placed.lines.map((line) =>
              line.points ? (
                <polyline
                  key={line.key}
                  className={lineActive(line, selectedEdge) ? "active" : undefined}
                  points={line.points}
                />
              ) : (
                <line
                  key={line.key}
                  className={lineActive(line, selectedEdge) ? "active" : undefined}
                  x1={line.x1}
                  y1={line.y1}
                  x2={line.x2}
                  y2={line.y2}
                />
              ),
            )}
          </svg>
          {placed.nodes.map((node) => (
            <button
              key={node.id}
              type="button"
              className={partyClass(node, selectedEdge)}
              style={{ left: node.x, top: node.y }}
              onClick={() => onSelect(paymentFor(graph, node.id))}
            >
              <PartyName name={node.name} />
            </button>
          ))}
        </div>
        <ul className="payments">
          {graph.edges.map((edge) => (
            <li key={edge.id}>
              <button
                type="button"
                className={edge.id === selected ? "selected" : undefined}
                onClick={() => onSelect(edge.id)}
              >
                <strong>{money(edge)}</strong>
                <span>{edge.time}</span>
              </button>
            </li>
          ))}
        </ul>
      </div>
      {expanded && selectedEdge ? (
        <div className="detail">
          <p className="who">
            <Marked graph={graph} id={selectedEdge.from} name={selectedEdge.src_name} /> paid{" "}
            <Marked graph={graph} id={selectedEdge.to} name={selectedEdge.dst_name} />.
          </p>
          <p>
            {money(selectedEdge)}
            {selectedEdge.time ? ` · ${selectedEdge.time}` : ""}
          </p>
          {selectedEdge.format ? <p>{selectedEdge.format}</p> : null}
        </div>
      ) : null}
    </aside>
  );
}

function Reply({ text }) {
  return blocks(text).map((block, index) => {
    if (block.type === "h") {
      return <h3 key={index}>{inline(block.text)}</h3>;
    }
    if (block.type === "ul") {
      return (
        <ul key={index}>
          {block.items.map((item, itemIndex) => (
            <li key={itemIndex}>{inline(item)}</li>
          ))}
        </ul>
      );
    }
    return <p key={index}>{inline(block.text)}</p>;
  });
}

function blocks(text) {
  const found = [];
  let list = null;
  function closeList() {
    if (!list) return;
    found.push({ type: "ul", items: list });
    list = null;
  }
  for (const line of String(text || "").split("\n")) {
    const heading = /^(#{1,3})\s+(.*)$/.exec(line);
    if (heading) {
      closeList();
      found.push({ type: "h", text: heading[2] });
      continue;
    }
    const item = /^[-*]\s+(.*)$/.exec(line);
    if (item) {
      list = list || [];
      list.push(item[1]);
      continue;
    }
    if (!line.trim()) {
      closeList();
      continue;
    }
    closeList();
    found.push({ type: "p", text: line });
  }
  closeList();
  return found;
}

function inline(text) {
  return String(text)
    .split(/(\*\*[^*]+\*\*)/g)
    .map((part, index) =>
      part.startsWith("**") && part.endsWith("**") ? (
        <strong key={index}>{part.slice(2, -2)}</strong>
      ) : (
        part
      ),
    );
}

function Marked({ graph, id, name }) {
  const mark = graph.nodes.find((node) => node.id === id)?.mark;
  if (!mark) return name;
  return <span className={mark}>{name}</span>;
}

function partyClass(node, edge) {
  const marks = ["party"];
  if (node.role === "subject") marks.push("subject");
  if (node.mark === "flagged") marks.push("flagged");
  else if (node.mark === "lead") marks.push("lead");
  if (edge && node.id === edge.from) marks.push("send");
  if (edge && node.id === edge.to) marks.push("receive");
  return marks.join(" ");
}

function lineActive(line, edge) {
  if (!edge) return false;
  return (
    (line.from === edge.from && line.to === edge.to) ||
    (line.from === edge.to && line.to === edge.from)
  );
}

function paymentFor(graph, accountId) {
  const touching = graph.edges.filter((edge) => edge.from === accountId || edge.to === accountId);
  const focal = touching.find((edge) => edge.focal);
  return (focal || touching[0] || graph.edges[0]).id;
}

function PartyName({ name }) {
  const text = String(name || "");
  const split = text.match(/^(.*)\s(#\S+)$/);
  if (!split) return text;
  return (
    <>
      <span>{split[1]}</span>
      <span>{split[2]}</span>
    </>
  );
}

function place(graph) {
  const subject = graph.nodes.find((node) => node.role === "subject") || graph.nodes[0];
  const others = graph.nodes.filter((node) => node.id !== subject.id);
  const ordered = [subject, ...others];
  const partyW = 208;
  const partyH = 72;
  const gap = 20;
  const pad = 16;
  const nodes = ordered.map((node, index) => ({
    ...node,
    x: pad,
    y: pad + index * (partyH + gap),
  }));
  const indexOf = new Map(nodes.map((node, index) => [node.id, index]));
  const seen = new Set();
  const lines = [];
  let rail = pad + partyW + 18;
  for (const edge of graph.edges) {
    const key = [edge.from, edge.to].sort().join("|");
    if (seen.has(key)) continue;
    seen.add(key);
    const first = indexOf.get(edge.from);
    const second = indexOf.get(edge.to);
    if (first == null || second == null || first === second) continue;
    const upper = nodes[Math.min(first, second)];
    const lower = nodes[Math.max(first, second)];
    const apart = Math.abs(first - second);
    if (apart === 1) {
      const x = upper.x + partyW / 2;
      lines.push({
        key,
        from: edge.from,
        to: edge.to,
        x1: x,
        y1: upper.y + partyH,
        x2: x,
        y2: lower.y,
      });
      continue;
    }
    const y1 = upper.y + partyH / 2;
    const y2 = lower.y + partyH / 2;
    const edgeX = upper.x + partyW;
    lines.push({
      key,
      from: edge.from,
      to: edge.to,
      points: `${edgeX},${y1} ${rail},${y1} ${rail},${y2} ${edgeX},${y2}`,
    });
    rail += 12;
  }
  const height = pad * 2 + ordered.length * partyH + Math.max(0, ordered.length - 1) * gap;
  return { nodes, lines, width: Math.max(pad * 2 + partyW, rail + pad), height };
}

function money(node) {
  if (node.amount == null || node.amount === "") return node.currency || "";
  const amount = Number(node.amount);
  const text = Number.isFinite(amount)
    ? amount.toLocaleString(undefined, { maximumFractionDigits: 2 })
    : String(node.amount);
  return `${text} ${node.currency}`.trim();
}
