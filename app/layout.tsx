import type { Metadata } from "next";
import "./globals.css";
import { Providers }  from "./providers";
import { Sidebar }    from "@/components/Sidebar";
import { TopBar }     from "@/components/TopBar";

export const metadata: Metadata = {
  title: "Orion Labs | Climate Risk Intelligence",
  description: "AI-powered climate risk scoring platform for insurance companies",
  icons: {
    icon: "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🔭</text></svg>",
  },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body style={{ backgroundColor: "var(--bg-page)", color: "var(--text-primary)" }} className="min-h-screen">
        <Providers>
          <div className="flex h-screen overflow-hidden">
            <Sidebar />
            <div className="flex flex-col flex-1 overflow-hidden">
              <TopBar />
              <main
                className="flex-1 overflow-auto"
                style={{ backgroundColor: "var(--bg-page)", padding: "32px" }}
              >
                {children}
              </main>
            </div>
          </div>
        </Providers>
      </body>
    </html>
  );
}
