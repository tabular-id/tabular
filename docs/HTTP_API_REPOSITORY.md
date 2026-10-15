# HTTP API folders linked to a code repository

A folder in the **APIs** sidebar can point at the code of the API it holds.
Tabular then reads that code to create every endpoint as a saved request,
links each endpoint to the database tables it uses in the diagram, and designs
integration tests that can span several APIs.

To create the folders, their repository links and endpoints for a whole repository
(one folder per microservice) in one step, see [REPO_SETUP.md](REPO_SETUP.md).

## Linking a repository

Right-click a folder and choose **Set Repository…** (or **Edit Repository…**).
The dialog is the same as the one for diagram groups
([DIAGRAM_GROUP_REPOSITORY.md](DIAGRAM_GROUP_REPOSITORY.md)):

| Field | Where it is stored | Notes |
|---|---|---|
| Git URL | In the HTTP collection (`http_collections/<workspace>.json`), exported with **Export All Data** | URLs with a password or token cannot be saved |
| Project folder | Only on this computer, in `{data dir}/diagram_repo_paths.json` | Used first when it exists; otherwise a private clone of the URL is used |

Folders with a repository show a git icon in the sidebar. **Open Folder
Location** opens the local project folder.

Server sync of HTTP collections sends requests and folder names only, so the
git URL of a folder stays on the devices where it was set or imported.

## The link to diagram groups

An HTTP API folder and a diagram group are linked when they point at the same
repository. Tabular compares the git URLs after normalising them, so
`https://github.com/Org/App.git`, `git@github.com:org/app` and
`ssh://git@github.com/org/app` are the same repository. When only a project
folder is set, its `origin` remote is used.

- In the APIs sidebar, **Linked Diagram Groups…** opens the diagram and focuses
  the linked group. With several groups, a list lets you pick one.
- In the diagram, right-click a group title and choose **HTTP API Folders…** to
  show the linked folder in the sidebar.

## Generate Endpoints from Repo (AI)

Right-click the folder and choose **Generate Endpoints from Repo (AI)**, or
press **Save & Generate Endpoints** in the repository dialog.

1. **Prepare the code.** Same as the diagram group scan: a local folder is read
   in place, a URL is cloned with depth 1 into `{data dir}/agent-workspace/repos/`.
2. **Find routes.** Every source file git knows about is searched for route
   definitions. Test files, `node_modules`, `vendor`, build output and files over
   1 MB are skipped. Supported without AI:

   | Language | Frameworks |
   |---|---|
   | JavaScript / TypeScript | Express, Koa, Fastify, Hono, NestJS, Next.js (`app/**/route.ts`, `pages/api`), SvelteKit (`+server.ts`) |
   | Python | FastAPI, Flask (including blueprints), Django `urls.py`, Django REST Framework routers |
   | PHP | Laravel (`routes/api.php` adds `/api`, `Route::prefix()->group`, `resource`, `apiResource`), Symfony attributes |
   | Ruby | Rails `config/routes.rb` (`namespace`, `scope`, `resources … only:`) |
   | Go | Gin, Echo, Fiber, chi, gorilla/mux, `net/http` (Go 1.22 `"GET /path"`) |
   | Rust | Axum, Actix Web, Rocket |
   | Java / Kotlin | Spring (`@RequestMapping` on the class plus `@GetMapping` …), JAX-RS |
   | C# | ASP.NET controllers (`[Route("api/[controller]")]`, `[HttpGet]`), minimal APIs with `MapGroup` |

3. **Find missed endpoints (AI).** When the AI backend can read the repository
   (Claude Code, Gemini CLI, or agy/Custom CLI in a private copy), it lists the
   endpoints the route search missed, such as routers mounted in another file.
4. **Document every endpoint (AI).** Endpoints are sent in batches of 20,
   grouped by file, so large APIs are documented completely. Three batches run
   at the same time by default; change **Parallel AI batches** (1 to 6) next to
   **Rescan** for the next run. Higher values are faster but use more of your AI
   quota. For each endpoint
   the AI fills in a name, description, path and query parameters, headers,
   auth (bearer, basic or API key), body type and example, a response example,
   status codes, the source line and the database tables it reads or writes.
   Table names are matched to the tables of the linked diagrams.

API providers (not CLI agents) cannot open files. They receive the source of
the route files, up to 60 KB per batch, with string literals that look like
passwords, tokens or keys replaced by `«redacted»`.

If no AI backend is ready or a batch fails, the endpoints from the route search
are still listed with their method and path; the window says so.

### Several folders at once

Each folder has its own generation job and window, so you can generate
endpoints for several folders or repositories at the same time. Starting a
folder again while its job is running only brings its window back. Close a
window to keep its job running in the background: a notification appears and
the window opens again when the endpoints are ready. **Cancel** stops the job.
All jobs together run at most 6 AI turns at once; extra batches wait for a
free turn.

### Reviewing and adding

Each row shows the method, path, name, the tables it uses, **AI** or **TEXT**
(route search only) and **exists** when the folder already has a request for the
same method and path. Click a row to see its parameters, body and response
examples. New endpoints start selected; existing ones do not.

- **Base URL** is detected from the code when possible (for example the port the
  server listens on), otherwise it follows the framework default.
- **Group into sub-folders by resource** creates one sub-folder per resource,
  such as `users` for `/api/v1/users/{id}`.
- **Link endpoints to diagram tables** is on when a linked diagram group exists.
- **Also generate business process** starts [business process
  generation](#business-processes-ai) for the linked diagrams that are open,
  right after the links are added. Diagrams that are not open get the links
  only; generate their processes from the diagram later.

**Add N endpoint(s)** creates the requests. Selected rows that already exist are
updated in place; tokens, passwords and API key values you already entered are
kept. Path parameters in the URL get their example value, and the route template
is kept with the request so later links still use `/users/{id}`.

**Link Endpoints to Diagram Tables** in the folder menu repeats the linking for
requests that already have tables, for example after you link a new diagram group.

## Endpoints in the database diagram

Linked endpoints are stored in the diagram, so they are saved to the diagram
file and the `diagram_by_tabular` table together with
groups and notes. A diagram that is not open is updated in its local file; open
it and save it to share the links.

By default endpoints are listed in the **API panel** on the left of the diagram
(the *Rail* mode), and the canvas stays a plain ERD:

- The panel groups endpoints by their main table (the table named in the path,
  otherwise the first table the endpoint writes), so `/panens`, `/panens/{id}`
  and `/panens_filter_date` all sit under `panens`. Each row shows the method,
  the full path and the operations the endpoint performs (**C**reate, **R**ead,
  **U**pdate, **D**elete).
- Click an endpoint to show its process card next to the tables it uses. The
  other tables are dimmed, the lines carry the operation and step numbers, and
  the process plays. Click the row again, or press Esc, to clear it.
- Hover a step to outline its table; hover a table to highlight the steps that
  use it.
- Each table header shows a badge such as **API 6 · CRUD**: six endpoints use
  the table, and together they create, read, update and delete it.
- The **CRUD matrix** tab counts endpoints per table and operation. Click a
  number to list those endpoints; click a table name to jump to it.
- Double-click a table name in the list to jump to that table. The filter box
  matches method, path, summary and table names. The chevron hides the panel.

The diagram menu controls how endpoints are shown:

| Setting | Options |
|---|---|
| **Show API endpoints** | Hides or shows cards and badges |
| **API endpoints as** | **Rail**: the API panel beside the diagram; the selected endpoint shows its process card next to its tables. **Badges**: only the blue **API n** badge on each table header. **Both** (default): the API panel and the badges |

Working with cards (only the endpoint selected in the API panel has a card; it
is placed next to its tables and always shows all its steps, at any zoom):

- Select an endpoint in the API panel to show its card and its lines to the
  tables. The view centres on the card at 50% zoom. Clicking the card plays its
  process and never hides it.
- Drag a card to move it. It keeps that place until the diagram tab is closed.
- Line colour follows the operation: blue for reads (arrow from the table),
  green for insert, update and upsert, red for delete (arrow to the table), grey
  when the operation is unknown. A table that is both read and written gets the
  write colour and a two-way arrow.
- Right-click a card for **Open Request**, **Copy as Mermaid**, **Generate
  Business Process (AI)** / **Regenerate** and **Remove Card**. Removing a card also unlinks its endpoint
  from the tables.
- With badges on, click a badge for the endpoint panel: filter, open an
  endpoint's request in the HTTP client, show its card, or unlink it from the
  table. Unlinking the last table of a card that has no steps removes the card.
- Diagram search (Cmd+F) finds endpoints by method or path. With the API panel
  on, the result opens the endpoint's card; with badges only, it jumps to the
  table.
- **Esc** or a click on empty canvas clears the selection.
- Links to tables that disappear from the schema are removed on the next schema
  sync. Steps that pointed at such a table keep their text. Focus and group tabs
  show the cards that touch their tables, read-only.

## Business processes (AI)

A card can show the business process behind its endpoint: the steps the code
runs in order, such as auth, validation, database reads and writes, calls to
other services, queue messages and the response, with the table, columns and
`path/file:line` of each step.

Start it from the diagram:

- right-click a group title and choose **Generate Business Process (AI)** for
  every card of that group's repository;
- right-click a card and choose **Generate Business Process (AI)**, or
  **Regenerate** when it already has steps;
- press **Regenerate** in the card's Process panel.

The same code preparation and AI backend as endpoint generation are used.
Cards are sent in batches of 6, grouped by source file, and share the limit of
6 AI turns at once with the other jobs. A progress window shows each step; it
has **Cancel**, and **Run in Background** hides it until a notification says the
job is done. When the job ends, the window lists how many cards were generated,
how many were unchanged and which failed.

- Table names in the steps are matched to the diagram. A step whose table is
  not in the diagram keeps its text without a line. New tables found in the
  steps are linked to the endpoint.
- Tabular records the files the AI read and a hash of their content. The next
  run skips cards whose files have not changed; **Regenerate** on a card always
  runs it again.
- API providers see route snippets only, not the whole repository. Their cards
  are labelled **partial**; a CLI agent (Claude Code, Gemini CLI) gives more
  complete processes.
- Without an AI backend, cards still show their tables from the links and say
  **No business process yet**.
- The Mac App Store build cannot start external processes, so generation is not
  available there.

### Playing a process

Double-click a card to focus it: the other cards and unrelated tables fade, the
view zooms to the card and its tables, and the process plays. The request
enters the card, each step lights up in turn, a particle runs along the line
to its table in the direction of the data, and the table's header and columns
pulse. The response leaves the card at the end. Playback stops by itself.

The bar under the card has Play / Pause (**Space**), Previous step, Next step,
speed (0.5×, 1×, 2×), Replay and Close. A card without steps plays one step per
linked table.

The **Process** panel opens for the selected card. It shows the summary, the
commit and time the process was generated from, **Tables involved** with
C / R / U / D badges and step numbers (click to jump to the table), and every
step with its detail, condition and source (click to play from that step).

### Export

- **Copy as Mermaid** on a card copies its process as a Mermaid `flowchart`:
  steps in order, tables as cylinders, arrows in the direction of the data,
  and conditional steps as dotted arrows labelled with the condition.
- **Export → Business processes (Mermaid .md)…** in the diagram toolbar writes
  one Markdown file with a flowchart per card, ordered by path.
- AI agents read the same processes through the MCP tool `describe_diagram`
  ([MCP.md](MCP.md)).

### Compatibility

Cards and steps are saved in the diagram (`flow_cards`). Older Tabular versions
open these diagrams but drop the cards when they save them. The cards come back
from the endpoint links the next time a current version opens the diagram; the
steps need to be generated again.

## Integration tests

Integration tests live in a tab per project. Click the **Integration Testing**
button in the top bar, next to the project name. The tab opens the project's
most recent suite, or a new empty suite when the project has none yet. A suite
is an ordered list of HTTP steps that pass data to each other.

### Steps, phases and groups

Each step has a **phase**, and the list always shows the execution order:

1. **Setup** steps run first, for example login or master data the tests need.
2. **Test** steps run next, group by group.
3. **Cleanup** steps run last and remove the test data.

**Groups** split a suite into business areas, for example *Purchasing*,
*Plant* and *Sales* in an ERP. Add groups in the left column, rename or reorder
them from their context menu, and assign a step to a group in its details.
Select a group and click **Run Purchasing** to test only that group. Steps the
group depends on run too, even when they belong to another group. For example,
*Plant* needs the purchase order that *Purchasing* creates, so that step runs
as well. These steps are shown in italics. Cleanup runs for every step that
created data.

### Data flow between steps

A step passes values to later steps with **Extract**, for example
`po_id = json:$.id`. Later steps use the value as `{{po_id}}` in the URL,
headers, body or auth. The **Data flow** column shows where each step's input
comes from and which steps use its output.

The **Checks** panel lists problems before you run:

- A step uses a variable that only a later step provides, or that nothing provides.
- A step creates data but no cleanup step removes it.
- Cleanup steps remove data in the wrong order. **Auto-order cleanup** removes
  dependent data first, in reverse order of creation.
- A step has no assertions.

### Editing a step

Click a step to edit its phase, group, extract variables, assertions and the
requirement it verifies. Double-click it, or use **Edit request in HTTP
client**, to open the request in a new HTTP client tab. Variables are filled
from the suite, the project environment and the last run, so you can send the
request from there. **Save** in that tab writes the request back to the step.

Add steps from the project's API collection with **Add step**. The host is
replaced by the folder's `base_url_<folder>` variable, and auth values are
replaced by variables such as `{{token}}`.

### Generating steps with AI

Open **AI** in the tab toolbar and describe the flow, for example
*"Purchasing: create vendor, create PO, approve, receive goods"*.

- **Add steps** appends steps to the suite, in the selected group. The AI
  reuses ids that existing steps already extract.
- **Add missing cleanup** adds cleanup steps for data that is not removed yet.
- **Design new suites** creates complete suites from all API endpoints of the
  project.

The AI receives the endpoint catalog (method, path, name, auth type, query
names, tables and redacted body examples, never auth values). It is asked to
use `{{run_id}}` in created names and codes, and to add a cleanup step for
every step that creates data. You can also start from a folder: right-click it
and choose **Generate Integration Tests (AI)…**.

### Running and cleaning up

**Run all** or **Run <group>** shows the number of steps, the steps that change
data, the cleanup steps, the environment and the target servers before
anything is sent. Running against an environment marked Production needs an
extra confirmation.

- Steps run strictly in order. A step whose variables are missing, because
  the step that provides them failed, is **Blocked** and not sent.
- A failing Setup step blocks the remaining tests. **Stop after first failure**
  in the menu does the same for test steps.
- **Cleanup always runs**, also when tests fail or you press **Stop**. A
  cleanup step only runs when the data it removes was actually created. Network
  errors are retried once. Only **Abort** skips cleanup.
- Data that could not be removed is listed as left behind, with its ids.
- When the project has database connections and the steps are linked to
  tables, Tabular then counts the rows that still contain the run id in those
  tables. The query is read-only.

Runs continue in the background when you switch tabs. They appear in the
Background Tasks panel.

### Report and validation evidence

The **Report** view shows the verdict and statistics of the last run. The
**History** view lists earlier runs. Statistics use ISTQB terms and follow the
ISO/IEC/IEEE 29119-3 test completion report:

| Item | Meaning |
|---|---|
| Verdict | PASS, PASS WITH DEVIATIONS (blocked steps or test data left behind), FAIL (below the pass criterion, or a setup step failed) or INCOMPLETE (stopped) |
| Passed, Failed, Error, Blocked, Skipped | Failed means assertions failed; Error means no response |
| Pass rate | Passed divided by executed steps |
| Execution rate | Executed steps divided by planned steps, without disabled steps |
| Pass criterion | Minimum pass rate, 100% unless changed in the menu |
| Per group | The same counts for each group |
| Response time | Minimum, average, p50, p90, p95 and maximum |
| Endpoint coverage | API endpoints of the project that a step exercises, and the ones without a test |
| Failures per endpoint | Endpoints that failed most |
| Flaky steps | Steps whose result changed between recent runs |

**Open full report** opens an HTML report you can print to PDF. **Export**
saves HTML, Markdown, JUnit XML or JSON. The report contains validation
evidence in the style of GAMP 5:

- who ran it, on which machine and when;
- the SHA-256 fingerprint of the suite definition that was run;
- expected versus actual results for every step;
- deviations and test data cleanup;
- a traceability matrix from step to endpoint, tables and requirement note;
- a review and approval table for signatures.

Tokens, passwords and other secret values are redacted in results and
reports.

### Command line and CI

```bash
tabular test list [--project <name>] [--json]
tabular test run "Procure to pay" --group Purchasing --junit report.xml --report report.html
```

`run` uses the project's active environment, or `--env <name>`. It refuses a
Production environment unless you pass `--allow-production`. The exit code is
0 for PASS and PASS WITH DEVIATIONS, and 1 otherwise.

### Storage

| What | Where |
|---|---|
| Suites | `{app data}/http_tests/<suite id>.json` |
| Run history (last 50 per suite) | `{app data}/http_tests/runs/<suite id>/<run id>.json` |

Assertions use `status`, `time_ms`, `body`, `header:<Name>` or
`json:$.path.to[0].field` with `equals`, `not_equals`, `contains`,
`not_contains`, `exists`, `not_exists`, `less_than`, `greater_than` or `matches`
(regular expression).
