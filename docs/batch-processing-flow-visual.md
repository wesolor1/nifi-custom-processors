# Batch processing flow

Short guide for **non-technical** readers. Optional diagram at the end for slides or documentation tools that support Mermaid.

---

## How this flow works (plain language)

- **One place for configuration**  
  Each batch flow (where data comes from, where it goes, when it runs, and other settings) is stored in a central table: **`nifi_flow_definition`**. Think of it as the “recipe book” for all similar jobs.

- **The schedule is written in advance**  
  Using the timing rules from that configuration, the system fills another table — **`nifi_schedule_occurrence`** — with one row per run that should happen. New rows start as **“waiting to start”** (*PENDING*).

- **NiFi picks up work when it is due**  
  A small automated step in **Apache NiFi** (your integration platform) reads the schedule table, finds runs that are **waiting**, and starts the right job. When a run starts, that row is marked as **“in progress”** (*RUNNING*).

- **One reusable template for many flows**  
  Instead of building dozens of separate NiFi diagrams, you use **one reusable process group** — the same steps, driven by the data passed in for each run (source, target, paths, and so on).

- **Custom “list” steps fix standard limitations**  
  Inside that template, **custom list processors** (for SMB and SFTP) connect to remote folders using those passed-in details. They can signal **success**, **no files to process**, or **failure** clearly — so operations and downstream steps always know what happened.

- **The database is updated when the run ends**  
  When the NiFi flow finishes, the matching row in **`nifi_schedule_occurrence`** is updated to **completed successfully** (*SUCCESS*) or **completed with a problem** (*FAIL*). That gives you a clear history of what ran and how it ended.

---

## Diagram (optional)

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
  "flowchart": { "htmlLabels": true, "curve": "basis", "padding": 16 }
}}%%
flowchart TB
  subgraph DB[" 🗄️ Database "]
    direction TB
    T1["nifi_flow_definition<br/><small>Source, target, schedule, …</small>"]
    T2["nifi_schedule_occurrence<br/><small>PENDING → RUNNING → SUCCESS / FAIL</small>"]
    T1 --> T2
  end

  subgraph PLAN[" 📅 Planning "]
    direction TB
    A["All flows configured<br/>in one place"]
    B["Scheduler fills<br/>next run times"]
    A --> B
  end

  subgraph NIFI[" ⚙️ Apache NiFi "]
    direction TB
    C["Starter reads<br/>PENDING work"]
    D["Mark run as<br/>RUNNING"]
    E["Hand off metadata<br/><small>Attributes on the FlowFile</small>"]
    C --> D --> E
  end

  subgraph CUSTOM[" ⭐ Custom list processors "]
    direction TB
    F["<b>One reusable process group</b><br/>ListSMBExtended · ListSFTPExtended<br/><small>Trigger · variables · failure path · “No Files” path</small>"]
  end

  subgraph CLOSE[" ✅ Finish "]
    direction TB
    G["Update database<br/>SUCCESS or FAIL"]
  end

  T1 -.->|defines| A
  B -.->|creates / updates rows| T2
  T2 -->|next due run| C
  E --> F
  F --> G
  G -.->|writes final status| T2

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

## One sentence you can paste into an email

**We store all flow settings and schedules in the database; NiFi starts each run when it is due, using one shared template and custom list steps so we always know if files were found, nothing was there, or something failed — then we write the final status back to the database.**
