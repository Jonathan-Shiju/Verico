import { Barlow_Condensed, Source_Sans_3 } from "next/font/google";

import { SiteHeader } from "./site-header";
import "./globals.css";

const display = Barlow_Condensed({
  subsets: ["latin"],
  weight: ["500", "600", "700"],
  variable: "--font-display",
});

const text = Source_Sans_3({
  subsets: ["latin"],
  weight: ["400", "600"],
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
