import Link from "next/link";

const POOLED = [
  ["12 → 1655", "1,206.81 EUR", "127427", "2022/09/01"],
  ["5464 → 1655", "1,722.80 EUR", "1615971", "2022/09/02"],
  ["1655 → 28717", "2,957.08 EUR", "1693885", "2022/09/02"],
];

const SPLIT = [
  ["31047 → 1241", "376.16 USD", "1117840", "2022/09/02"],
  ["1241 → 117453", "161.85 USD", "1828122", "2022/09/02"],
  ["1241 → 216645", "210.05 USD", "2121045", "2022/09/04"],
];

const POINTS = [
  ["Payments forms the chain", "Receipients and Senders - linked via payments"],
  ["Investigate Leads", "Leads identified are drilled down to pinpoint frauds"],
  ["Follow the money", "Leads are built by tracing the payment trails"],
  ["Final Call is yours", "Leads do not make the final call, you do."],
  ["Document faster", "Automate generating case files on frauds"],
];

export default function HomePage() {
  return (
    <div className="home">
    <div className="rise">
    <section className="landing" aria-label="Verico">
      <div className="landing-copy">
        <svg viewBox="0 0 16 48" aria-hidden="true">
          <line x1="8" y1="1" x2="8" y2="34" />
          <polygon points="3,34 8,47 13,34" />
        </svg>
        <p>Transactions take advantage of a graph, where one payment joins the next.</p>
        <p>Finding leads gets easier, and fraud is analysed at speed.</p>
      </div>
    </section>
    <div className="bill">
      <h1 className="landing-name">Verico</h1>
    <main className="sheet">
      <section className="face" aria-label="Waybill for a prepared lead">
        <div className="mast">
          <p className="form-id">VRC-01</p>
        </div>
        <div className="pitch">
          <p>
            Verico reads a stream of payments as accounts joined together. It prepares the leads,
            walks them with an investigator, and writes a case book only when they keep a lead and
            ask for the record.
          </p>
          <Link href="/method">
            Read the routing
            <svg viewBox="0 0 48 16" aria-hidden="true">
              <line x1="1" y1="8" x2="34" y2="8" />
              <polygon points="34,3 47,8 34,13" />
            </svg>
          </Link>
        </div>
        <div className="cargo">
          <div className="loop">
            <h2>Loop</h2>
            <p>Money left 54219 and came back from 254958. The return is the lead.</p>
            <figure className="lane">
              <svg viewBox="0 0 640 250" role="img" aria-label="Payment 325411 goes from 54219 to 254958. Payment 3932103 returns, and that return is the lead.">
                <text className="station" x="8" y="36">54219</text>
                <text className="account" x="8" y="58">81464BEA1</text>
                <text className="station" x="632" y="36" textAnchor="end">254958</text>
                <text className="account" x="632" y="58" textAnchor="end">8146532E1</text>
                <line className="out" x1="158" y1="28" x2="462" y2="28" />
                <polygon className="out-head" points="462,20 486,28 462,36" />
                <text className="legend" x="320" y="86" textAnchor="middle">
                  325411 · 0.289661 BTC · 2022/09/01 00:21
                </text>
                <text className="mark" x="320" y="132" textAnchor="middle">Lead</text>
                <text className="station" x="8" y="186">54219</text>
                <text className="station" x="632" y="186" textAnchor="end">254958</text>
                <line className="back" x1="486" y1="176" x2="158" y2="176" />
                <polygon className="back-head" points="158,168 134,176 158,184" />
                <text className="legend return" x="320" y="214" textAnchor="middle">
                  3932103 · 0.089911 BTC · 2022/09/08 09:29
                </text>
                <text className="caption" x="8" y="242">Window from 2022/09/01</text>
              </svg>
            </figure>
            <div className="invite">
              <p>Walk this lead. The workspace is the demo.</p>
              <Link className="stamp" href="/investigate">
                Take the demo
              </Link>
            </div>
          </div>
          <div className="leads">
            <section>
              <h2>Pooled</h2>
              <p>Payments from more than one account arrive, then one payment leaves.</p>
              <ul>
                {POOLED.map(([path, amount, id, time]) => (
                  <li key={id}>
                    <span>{path}</span>
                    <b>{amount}</b>
                    <em>{time}</em>
                    <em>{id}</em>
                  </li>
                ))}
              </ul>
            </section>
            <section>
              <h2>Split</h2>
              <p>One payment arrives, then leaves toward more than one account.</p>
              <ul>
                {SPLIT.map(([path, amount, id, time]) => (
                  <li key={id}>
                    <span>{path}</span>
                    <b>{amount}</b>
                    <em>{time}</em>
                    <em>{id}</em>
                  </li>
                ))}
              </ul>
            </section>
          </div>
        </div>
        <div className="provides">
          <h2>What Verico provides</h2>
          <ul>
            {POINTS.map(([title, text]) => (
              <li key={title}>
                <strong>{title}</strong>
                <span>{text}</span>
              </li>
            ))}
          </ul>
        </div>
      </section>
    </main>
    </div>
    </div>
    </div>
  );
}
