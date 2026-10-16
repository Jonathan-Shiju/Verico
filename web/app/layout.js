import { Familjen_Grotesk, Fraunces } from "next/font/google";

import { SiteHeader } from "./site-header";
import "./globals.css";

const display = Fraunces({
  subsets: ["latin"],
  variable: "--font-display",
});

const text = Familjen_Grotesk({
  subsets: ["latin"],
  variable: "--font-text",
});

export const metadata = {
  title: "Verico",
  description: "An investigation assistant for connected payments.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en" className={`${display.variable} ${text.variable}`} suppressHydrationWarning>
      <body suppressHydrationWarning>
        <SiteHeader />
        {children}
      </body>
    </html>
  );
}
