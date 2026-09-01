import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "InferX Operations Center",
  description: "Real-Time AI Inference Gateway & SSE Telemetry Pipeline",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body style={{ margin: 0, padding: 0, backgroundColor: "#060913" }}>
        {children}
      </body>
    </html>
  );
}
