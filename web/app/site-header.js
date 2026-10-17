"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const LINKS = [
  { href: "/", label: "Idea" },
  { href: "/method", label: "Method" },
  { href: "/investigate", label: "Workspace" },
  { href: "/blog", label: "Blog" },
];

export function SiteHeader() {
  const path = usePathname();
  return (
    <header className="route">
      <Link href="/" className="carrier">
        Verico
      </Link>
      <nav aria-label="Routing">
        {LINKS.map((link, index) => {
          const current = path === link.href;
          return (
            <Link
              key={link.href}
              href={link.href}
              aria-current={current ? "page" : undefined}
            >
              <span>{String(index + 1).padStart(2, "0")}</span>
              {link.label}
            </Link>
          );
        })}
      </nav>
    </header>
  );
}
