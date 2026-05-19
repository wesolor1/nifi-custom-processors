# Batch processing flow — visual guide

Use this file for **stakeholder-friendly** diagrams. The Mermaid block below uses **colors and short labels** so it reads well in GitHub, GitLab, Notion, or [Mermaid Live Editor](https://mermaid.live).

---

## Draw.io vs other tools (quick recommendation)

| Tool | Best for | Mermaid paste |
|------|-----------|----------------|
| **[Mermaid Live](https://mermaid.live)** | **Best first step** — paste code, pick theme, export **SVG** or **PNG** with consistent colors | Native — looks as designed |
| **Draw.io (diagrams.net)** | Manual polish, exact branding, drag icons | Mermaid is **imported as editable shapes**; **theme/colors often look plain or differ** from Mermaid Live — not a bug, just different engine |
| **GitHub / GitLab** | Docs next to code | Renders Mermaid in `.md` files well |
| **Notion / Confluence** | Wikis for mixed audiences | Good Mermaid support |
| **Excalidraw** | Workshops, “sketch” feel | No Mermaid — draw by hand or **paste SVG** exported from Mermaid Live |

**Practical workflow for Draw.io:** paste the diagram in **Mermaid Live** → **Actions → Export SVG** → in Draw.io **File → Import** the SVG. You keep the colors and can still add logos/text boxes on top.

---

## Colored flowchart (copy everything inside the fence)

Paste into [mermaid.live](https://mermaid.live) for the nicest preview, or into any Markdown viewer that supports Mermaid.

```mermaid
%%{init: {
  "theme": "base",
  "themeVariables": {
    "fontFamily": "Segoe UI, system-ui, sans-serif",
    "fontSize": "15px",
    "primaryColor": "#e3f2fd",
    "primaryTextColor": "#0d47a1",
    "primaryBorderColor": "#1565c0",
    "lineColor": "#455a64",
    "secondaryColor": "#fff8e1",
    "tertiaryColor": "#e8f5e9"
  },
  "flowchart": { "htmlLabels": true, "curve": "basis", "padding": 12 }
}}%%
flowchart TB
  subgraph DB[" 🗄️ Database "]
    T1["nifi_flow_definition<br/><small>Source, target, schedule, …</small>"]
    T2["nifi_schedule_occurrence<br/><small>PENDING → RUNNING → SUCCESS / FAIL</small>"]
  end

  subgraph PLAN[" 📅 Planning "]
    A["All flows configured<br/>in one place"]
    B["Scheduler fills<br/>next run times"]
    A --> B
  end

  subgraph NIFI[" ⚙️ Apache NiFi "]
    C["Starter reads<br/>PENDING work"]
    D["Mark run as<br/>RUNNING"]
    E["One reusable process group<br/><small>Metadata on the FlowFile</small>"]
    C --> D --> E
  end

  subgraph CUSTOM[" ⭐ Custom list processors "]
    F["ListSMBExtended<br/>ListSFTPExtended<br/><small>Trigger • variables • errors • “no files”</small>"]
  end

  subgraph CLOSE[" ✅ Finish "]
    G["Update database<br/>SUCCESS or FAIL"]
  end

  T1 -.-> A
  B -.-> T2
  T2 <-->|read / update| C
  D -.-> T2
  E --> F
  F --> G
  G -.-> T2

  classDef db fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
  classDef plan fill:#fff8e1,stroke:#f57c00,stroke-width:2px,color:#e65100
  classDef nifi fill:#fce4ec,stroke:#c2185b,stroke-width:2px,color:#880e4f
  classDef star fill:#e8f5e9,stroke:#2e7d32,stroke-width:3px,color:#1b5e20
  classDef done fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px,color:#4a148c

  class T1,T2 db
  class A,B plan
  class C,D,E nifi
  class F star
  class G done
```

---

## Why Draw.io can look “wrong” with Mermaid

- Draw.io **translates** Mermaid into its own shapes; not every Mermaid style or `classDef` maps 1:1.
- Long labels in one line can **stretch boxes** — the diagram above uses `<br/>` and `<small>` for cleaner layout where supported.

If you need **pixel-perfect** slides, export **SVG from Mermaid Live** and import into Draw.io (or PowerPoint).

---

## One-line summary (for slides)

**Database holds definitions and schedule rows → NiFi picks up due work → custom list processors run with that metadata → database records SUCCESS or FAIL.**
