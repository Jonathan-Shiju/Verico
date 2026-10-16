const STEPS = [
  {
    title: "Payments become a connected record",
    text: "Verico takes a stream of payments and keeps them as accounts joined by those payments.",
  },
  {
    title: "The stream is reduced",
    text: "It reduces that stream to the payments that connect, so the next step is not a pile of rows.",
  },
  {
    title: "Leads are prepared",
    text: "Money that left an account and later came back, and payments that add up to one another, are ready to open. An account with many counterparties is marked, and that mark is not itself a lead.",
  },
  {
    title: "An investigator reads the lead",
    text: "They open a lead, or ask for the accounts that dealt with one already open, and read who paid whom.",
  },
  {
    title: "The path can be walked, and written down",
    text: "The same workspace follows the payments. When the investigator keeps the lead and asks for the record, Verico writes a case book. A dismiss does not open one.",
  },
];

export const metadata = {
  title: "Method · Verico",
};

export default function MethodPage() {
  return (
    <main className="page method">
      <p className="eyebrow">The method</p>
      <h1>What Verico does with a stream of payments.</h1>
      <p className="lede">
        The work moves from the whole stream to a lead a person can read, and
        from that reading to a written record when they ask for one.
      </p>
      <ol>
        {STEPS.map((step, index) => (
          <li key={step.title}>
            <span>{index + 1}</span>
            <div>
              <h2>{step.title}</h2>
              <p>{step.text}</p>
            </div>
          </li>
        ))}
      </ol>
    </main>
  );
}
