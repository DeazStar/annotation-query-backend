import Link from "next/link";

const includedItems = [
  "FastAPI application and API surface",
  "Celery worker flow and async task lifecycle",
  "Redis, MongoDB, Socket.IO, and backend selection",
  "Setup, operations, and contributor guidance",
];

const summaryCards = [
  ["Architecture", "System topology, request flow, and execution boundaries."],
  ["Docs", "A maintained handbook route at /docs with MDX content and sidebar navigation."],
  ["Reference", "Start with the canonical upstream repo and document current behavior from code."],
] as const;

export default function Home() {
  return (
    <main
      style={{
        minHeight: "100vh",
        position: "relative",
        padding: "2rem 1.25rem 3rem",
        overflow: "hidden",
        background: "transparent",
      }}
    >
      <section
        style={{
          maxWidth: 1180,
          margin: "0 auto",
          display: "grid",
          gap: "1.5rem",
        }}
      >
        <div
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: "0.75rem",
            border: "1px solid var(--aqb-border)",
            background: "var(--aqb-panel-soft)",
            padding: "0.55rem 0.85rem",
            fontFamily: "var(--fd-font-mono)",
            fontSize: "0.74rem",
            letterSpacing: "0.14em",
            textTransform: "uppercase",
          }}
        >
          <span>Rejuve Bio</span>
          <span style={{ opacity: 0.5 }}>/</span>
          <span>Annotation Query Backend</span>
        </div>

        <div
          style={{
            display: "grid",
            gap: "1.75rem",
            gridTemplateColumns: "minmax(0, 1.4fr) minmax(280px, 0.9fr)",
          }}
        >
          <div
            style={{
              display: "grid",
              gap: "1.25rem",
              padding: "clamp(1.4rem, 4vw, 2.4rem)",
              background: "var(--aqb-panel)",
              border: "1px solid var(--aqb-border)",
              boxShadow: "0 28px 80px var(--aqb-shadow)",
            }}
          >
            <p
              style={{
                margin: 0,
                fontFamily: "var(--fd-font-mono)",
                fontSize: "0.78rem",
                letterSpacing: "0.16em",
                textTransform: "uppercase",
                color: "var(--aqb-accent)",
              }}
            >
              Internal engineering handbook
            </p>
            <h1
              style={{
                margin: 0,
                fontSize: "clamp(1.6rem, 4vw, 2.4rem)",
                lineHeight: 0.9,
                letterSpacing: "-0.06em",
                maxWidth: 760,
              }}
            >
              Read the backend as a system, not just a set of files.
            </h1>
            <p
              style={{
                margin: 0,
                maxWidth: 720,
                fontSize: "1.05rem",
                lineHeight: 1.8,
                color: "var(--aqb-muted)",
              }}
            >
              This docs site is the working handbook for the Rejuve Bio
              annotation-query-backend. It is meant to help current developers,
              interns, and new contributors understand architecture, setup,
              runtime behavior, and the major system boundaries.
            </p>

            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: "0.85rem",
              }}
            >
              <Link
                href="/docs"
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  justifyContent: "center",
                  padding: "1rem 1.3rem",
                  textDecoration: "none",
                  fontWeight: 700,
                  background: "var(--aqb-accent)",
                  color: "#fff7ed",
                  minWidth: 190,
                }}
              >
                Open Handbook
              </Link>
              <a
                href="https://github.com/rejuve-bio/annotation-query-backend"
                target="_blank"
                rel="noreferrer"
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  justifyContent: "center",
                  gap: "0.5rem",
                  padding: "1rem 1.3rem",
                  textDecoration: "none",
                  border: "1px solid rgba(154, 52, 18, 0.22)",
                  color: "var(--aqb-ink)",
                  minWidth: 220,
                  background: "var(--aqb-panel-soft)",
                }}
              >
                <svg width="18" height="18" viewBox="0 0 16 16" fill="currentColor"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27s1.36.09 2 .27c1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8"/></svg>
                View on GitHub
              </a>
            </div>
          </div>

          <div
            style={{
              display: "grid",
              gap: "1rem",
              alignContent: "start",
            }}
          >
            {includedItems.map((item) => (
              <div
                key={item}
                style={{
                  padding: "1.1rem 1.1rem",
                  background: "var(--aqb-panel-strong)",
                  border: "1px solid var(--aqb-border-soft)",
                }}
              >
                <span
                  style={{
                    display: "block",
                    fontFamily: "var(--fd-font-mono)",
                    fontSize: "0.74rem",
                    letterSpacing: "0.14em",
                    textTransform: "uppercase",
                    color: "var(--aqb-accent)",
                    marginBottom: "0.45rem",
                  }}
                >
                  Included
                </span>
                <strong style={{ fontSize: "1rem", lineHeight: 1.5 }}>{item}</strong>
              </div>
            ))}
          </div>
        </div>

        <div
          style={{
            display: "grid",
            gap: "1rem",
            gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          }}
        >
          {summaryCards.map(([title, text]) => (
            <div
              key={title}
              style={{
                padding: "1.15rem",
                background: "var(--aqb-panel-strong)",
                borderTop: "4px solid var(--aqb-accent-soft)",
                borderRight: "1px solid var(--aqb-border-soft)",
                borderBottom: "1px solid var(--aqb-border-soft)",
                borderLeft: "1px solid var(--aqb-border-soft)",
              }}
            >
              <h2 style={{ margin: "0 0 0.6rem 0", fontSize: "1.05rem" }}>{title}</h2>
              <p style={{ margin: 0, lineHeight: 1.7, color: "var(--aqb-muted)" }}>{text}</p>
            </div>
          ))}
        </div>

        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: "1rem",
            flexWrap: "wrap",
            alignItems: "center",
            borderTop: "1px solid var(--aqb-border-soft)",
            paddingTop: "1rem",
            color: "var(--aqb-muted)",
            fontFamily: "var(--fd-font-mono)",
            fontSize: "0.74rem",
            letterSpacing: "0.12em",
            textTransform: "uppercase",
          }}
        >
          <span>Built with Farming Labs docs on Next.js</span>
          <Link
            href="/docs"
            style={{
              textDecoration: "none",
              color: "var(--aqb-accent)",
            }}
          >
            Start at /docs
          </Link>
        </div>
      </section>
    </main>
  );
}
