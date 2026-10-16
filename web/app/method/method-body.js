"use client";

import { useEffect, useState } from "react";

const STEPS = [
  {
    id: "joined",
    point: "The joins",
    image: "payments-stay-joined.png",
    title: "Payments stay joined",
    text: "A payment is the link between the account that sent the money and the account that received it. Verico keeps that link, so the stream can be followed from one account to the next instead of read as a pile of separate rows.",
  },
  {
    id: "reduced",
    point: "Less to read",
    image: "stream-made-smaller.png",
    title: "The stream is made smaller",
    text: "A full stream is too much to read one row at a time. Verico keeps the payments that continue a connection and sets the rest aside. What remains is the part a person can actually follow.",
  },
  {
    id: "leads",
    point: "The leads",
    image: "patterns-as-leads.png",
    title: "A few patterns are prepared as leads",
    text: "Three shapes are ready to open. Money that left an account and later came back. Payments from more than one account that arrive, and then one payment that leaves. One payment that arrives, and then leaves toward more than one account. An account that deals with many others can be marked. That mark is not a lead.",
  },
  {
    id: "reading",
    point: "Who paid whom",
    image: "person-opens-lead.png",
    title: "A person opens the lead",
    text: "An investigator opens a lead, or asks which accounts dealt with one that is already open. They read the payments in order: who paid, who received, how much, and when.",
  },
  {
    id: "record",
    point: "The case book",
    image: "record-only-when-asked.png",
    title: "A record is written only when they ask",
    text: "In the same workspace they walk from one payment to the next. If they keep the lead and ask for the record, Verico writes a case book. If they set the lead aside, nothing is written. Verico does not decide that a payment is fraud.",
  },
];

export function MethodBody() {
  const [current, setCurrent] = useState(STEPS[0].id);

  useEffect(() => {
    const mark = () => {
      const line = window.innerHeight * 0.34;
      let active = STEPS[0].id;
      for (const step of STEPS) {
        const node = document.getElementById(step.id);
        if (node && node.getBoundingClientRect().top <= line) active = step.id;
      }
      setCurrent(active);
    };
    mark();
    window.addEventListener("scroll", mark, { passive: true });
    return () => window.removeEventListener("scroll", mark);
  }, []);

  return (
    <main className="method">
      <nav className="method-rail" aria-label="How it works">
        {STEPS.map((step, index) => (
          <a
            key={step.id}
            href={`#${step.id}`}
            aria-current={current === step.id ? "true" : undefined}
            onClick={() => setCurrent(step.id)}
          >
            <span>{String(index + 1).padStart(2, "0")}</span>
            {step.point}
          </a>
        ))}
      </nav>
      <div className="method-read">
        <h1>How a stream becomes a case book.</h1>
        <p className="lede">
          The work moves from the whole stream, to a lead a person can read, to a
          written record only when they keep that lead and ask for it.
        </p>
        {STEPS.map((step) => (
          <section id={step.id} key={step.id} className="method-step">
            <div className="method-copy">
              <h2>{step.title}</h2>
              <p>{step.text}</p>
            </div>
            <img src={`/method/${step.image}`} alt="" />
          </section>
        ))}
      </div>
    </main>
  );
}
