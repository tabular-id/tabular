# Set up from a Git folder

Pick a git repository and Tabular builds everything around it in one step: a project,
the database connections for each environment, an API folder for each service with its
endpoints, and a database diagram with one group per service. Afterwards Tabular checks
the repository in the background and updates all of it when the code or the config
changes.

## Start

Use any of these:

- **Add New Connection** dialog (the **+** in the Connections sidebar): switch to **Git
  Folder**, pick the folder, and choose a **Project Name**. Pick an existing project from
  the list, or type a new name to create one. Everything is set up inside that project,
  so you do not fill in host, port and credentials yourself.
- **Git** tab > **+** > **Add folder & set up everything…**
- **Connections** sidebar: right-click the **+** button, or the empty area of the list,
  and choose **Add from Git Folder…**
- **Git** tab: right-click a repository and choose **Set up connections, APIs &
  diagrams**.

The scan runs in the background and shows up under **Background Processes**. When it
finishes, a message lists what was created. To check the plan before anything is
written, turn on **Review before applying** in Preferences > Git.

## What Tabular reads

| Source | What becomes a connection |
|---|---|
| `.env`, `.env.<name>`, `.envrc`, `<env>.env` | URL variables (`DATABASE_URL`, `MYSQL_URL`, `REDIS_URL`, `MONGO_URI`, …), ADO.NET connection strings, and variable groups such as `DB_HOST` / `DB_PORT` / `DB_DATABASE` / `DB_USERNAME` / `DB_PASSWORD` / `DB_CONNECTION`, `PGHOST` / `PGUSER` / …, `POSTGRES_*`, `MYSQL_*`, `REDIS_*` |
| `docker-compose.yml`, `compose.yaml` | Containers built from a `postgres`, `mysql`, `mariadb`, `mongo`, `redis` or SQL Server image, on the port published to your machine |
| Spring Boot `application*.properties` / `application*.yml` | `spring.datasource.*`, `spring.r2dbc.*`, `spring.data.mongodb.*`, `spring.data.redis.*` |
| .NET `appsettings*.json` | Every entry under `ConnectionStrings` |
| Rails `config/database.yml` | Each environment, including `ENV[...]` and `ENV.fetch(...)` values |
| TypeORM `ormconfig.json`, Prisma `schema.prisma` | The configured database (Prisma only when the URL is written in the file) |

Template files such as `.env.example` or `.env.sample` are skipped. Folders like
`node_modules`, `vendor`, `target` and hidden folders are skipped too.

### Environments

The file name sets the environment: `.env` is Development, `.env.staging` is Staging,
`.env.production` or `prod.env` is Production, `.env.test` is Testing, and `.env.local`
or `.env.docker` is Local. A plain `.env` inside a folder such as `deploy/production/`
takes the folder's environment. Spring profiles (`application-prod.yml`) and .NET
files (`appsettings.Staging.json`) work the same way. Containers from docker-compose are
Local.

When a `.env` points at a compose container by its service name (`DB_HOST=postgres`),
Tabular connects to `localhost` on the port that container publishes.

### Credentials

Values that look like placeholders (`<password>`, `changeme`, `your_password`, an
unresolved `${VAR}`) still create the connection, without the password. The setup
window marks it **fill in the credentials**. Passwords go to the OS keychain like any
other connection.

## What gets created

| Item | Details |
|---|---|
| Project | The project named in the Add New Connection dialog. Otherwise the project selected in the project switcher, if any. Otherwise the project whose git URL matches, otherwise a new project named after the repository. Reused on later syncs. |
| Environments | One per environment found. `.env` variables become project variables; keys that look secret (`*PASSWORD*`, `*SECRET*`, `*TOKEN*`, `*_KEY`, database URLs) are stored in the keychain. |
| Connections | In the project's connection folder, coloured by environment, and ticked under the matching project environment. A connection you already had for the same server and database is reused instead of duplicated. |
| API folders | One folder per service in the project's HTTP collection, linked to the repository and the service folder. A folder with the same name, or one already linked to this repository, is reused. Endpoints are grouped in sub-folders by resource. |
| Diagram | For each development database, the diagram opens and gets one group per service, holding the tables that service's code uses. An existing group with the same name, or already linked to this repository, is reused. |
| Git tab | The repository is listed under the project. |

### Monolith or microservices

A service is a folder with a project manifest (`package.json`, `go.mod`, `pom.xml`,
`Cargo.toml`, `composer.json`, `pyproject.toml`, `*.csproj`, …) or a `build` context in
docker-compose, that contains HTTP routes. Each route belongs to the deepest service
folder above it. One service means a monolith, named after the repository. Several mean
microservices: each gets its own API folder, its base URL (from the published compose
port, `APP_URL`, `BASE_URL` or `PORT` in its `.env`), and its own diagram group.

Routes are found with the same scanner as **Generate Endpoints from Repo** (Express,
NestJS, Next.js, FastAPI, Flask, Django, Laravel, Rails, Go, Axum, Actix, Spring,
ASP.NET and more). Without AI the requests have the method, path and path parameters.
Turn on **Document new endpoints with AI** to run Generate Endpoints afterwards for
folders that received new endpoints; it fills parameters, bodies, examples and the
tables each endpoint uses, and links them in the diagram.

## Staying in sync

For every repository you set up, Tabular checks in the background whether the current
commit, the working tree, or one of the env and config files changed. It checks every
5 minutes by default and when Tabular comes back to the front. Nothing is fetched or
pulled; Tabular reads the folder as it is on disk.

When something changed, Tabular runs the scan again and:

- adds connections that are new, and updates host, port, user, database and password of
  connections whose values changed (a blank or placeholder password never overwrites
  one you typed);
- adds new endpoints to the API folders and new service folders, leaving requests you
  edited or documented with AI as they are;
- adds new tables to the service groups of the diagram.

Nothing is deleted. A connection or service that disappears from the repository is
marked **no longer in the repository** in the setup window, where **Forget missing
items** clears the marks.

A short message tells you what changed. With **Review before applying** on, Tabular
only tells you that the repository changed; open the setup window and choose **Apply
detected changes**.

## The setup window

Right-click the repository in the Git tab and choose **Repository setup…**, or click the
finished entry in Background Processes. It shows the project, the last sync, every
connection with its environment and source file, every API folder, and the diagrams.

- **Keep in sync with the repository** turns the background check on or off for this
  repository.
- **Sync now** scans again right away.
- **Stop managing** forgets the setup. Connections, API folders and diagrams stay.

## Preferences

Preferences > Git > Repository setup:

| Setting | Default |
|---|---|
| Check for changes every | 5 min (0 = only when Tabular regains focus) |
| Review before applying | Off |
| Open diagrams after setup | On |
| Document new endpoints with AI | Off |

## Privacy and limits

- Values from `.env` files are never written to logs and never sent to an AI backend.
  AI documentation reads code, the same way Generate Endpoints does.
- Diagram changes are saved on this computer. They reach the `diagram_by_tabular`
  table in the database only when you save the diagram yourself.
- The setup list lives in `repo_setups.json` in the Tabular data folder. It is personal
  and not synced, because it holds local paths and connection ids.
- Business process (flow) diagrams are not generated automatically. Use **Generate
  Business Process** on a diagram group.
- Kubernetes manifests, Helm values, Terraform and secret managers are not read.
