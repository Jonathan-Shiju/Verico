import Link from "next/link";

const FEATURES = [
  {
    title: "Connected payments",
    text: "Each payment names two accounts. The next payment continues from one of them, so a path is visible instead of a row in a file.",
  },
  {
    title: "Leads ready to open",
    text: "Money that left and later came back, and payments that add up to one another, are prepared as leads an investigator can open.",
  },
  {
    title: "A walk along the path",
    text: "From one payment, Verico follows the payments connected to it, so the investigator can see how the money moved.",
  },
  {
    title: "A reading, kept or set aside",
    text: "The investigator keeps a lead for a person to review, or dismisses it as ordinary. Verico does not decide that a payment is fraud.",
  },
  {
    title: "A case book after a keep",
    text: "When the investigator asks, the conversation and the payments become a case book. A dismiss does not open one.",
  },
];

export default function HomePage() {
  return (
    <main className="page">
      <section className="hero">
        <p className="eyebrow">Payment investigation</p>
        <h1>Fraud hides in the way payments connect.</h1>
        <p className="lede">
          One payment is rarely the whole story. It leaves one account and
          arrives in another, and a later payment continues from there. A loop,
          a pool of incoming payments, or a split into several outgoing
          payments is a path through accounts. Verico reads those paths with
          an investigator.
        </p>
        <Path />
      </section>
      <section className="features">
        <h2>What you work with</h2>
        <ul>
          {FEATURES.map((feature) => (
            <li key={feature.title}>
              <h3>{feature.title}</h3>
              <p>{feature.text}</p>
            </li>
          ))}
        </ul>
        <p className="next-step">
          <Link href="/method">See how Verico works</Link>
        </p>
      </section>
    </main>
  );
}

function Path() {
  return (
    <svg className="path" viewBox="0 0 640 120" role="img" aria-label="Four accounts joined by payments">
      <line x1="70" y1="60" x2="210" y2="36" />
      <line x1="210" y1="36" x2="360" y2="78" />
      <line x1="360" y1="78" x2="520" y2="48" />
      <line x1="520" y1="48" x2="210" y2="36" />
      <circle cx="70" cy="60" r="18" />
      <circle cx="210" cy="36" r="18" />
      <circle cx="360" cy="78" r="18" />
      <circle cx="520" cy="48" r="18" />
    </svg>
  );
}
