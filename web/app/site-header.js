"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const LINKS = [
  { href: "/", label: "Idea" },
  { href: "/method", label: "Method" },
  { href: "/investigate", label: "Workspace" },
];

export function SiteHeader() {
  const path = usePathname();
  return (
    <header className="site-header">
      <Link href="/" className="mark">
        Verico
      </Link>
      <nav>
        {LINKS.map((link) => {
          const current = path === link.href;
          return (
            <Link
              key={link.href}
              href={link.href}
              aria-current={current ? "page" : undefined}
            >
              {link.label}
            </Link>
          );
        })}
      </nav>
    </header>
  );
}
