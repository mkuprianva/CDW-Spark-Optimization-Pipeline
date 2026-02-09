# CDWPipe (Scala / Spark / Delta for Databricks)

## Prereqs
- JDK 17+
- sbt 1.12+

## Build
- Compile: `sbt compile`
- Test: `sbt test`
- Fat JAR (for Databricks): `sbt clean assembly`
  - Output: `target/scala-2.13/CDWPipe_2.13-0.1.0-SNAPSHOT.jar`

## Databricks notes
- Spark + Delta are marked as `Provided` (Databricks runtime supplies them).
- Arrow is typically provided by Spark; you usually don’t need to add an explicit Arrow dependency unless you’re directly using Arrow APIs.

### Databricks Runtime 16.4 alignment
- This project is set up to match DBR 16.4 (Scala 2.13 image):
  - Scala `2.13.10`
  - Spark `3.5.2`
  - Delta `3.3.1`

### "No target" troubleshooting
You’ll typically see "no target" in one of two places:

1) **Databricks Asset Bundles (CLI)**
- Symptom: `databricks bundle validate/deploy/run` fails with a message like "no target specified".
- Fix:
  - Use the included `databricks.yml` (it defines a default `dev` target), or pass a target explicitly: `databricks bundle validate -t dev`.
  - Update `targets.dev.workspace.host` in `databricks.yml` to your workspace URL.

2) **VS Code Metals (Scala build targets)**
- Symptom: Metals shows "No build target" / can’t run or compile from the editor.
- Fix:
  - Ensure JDK + sbt are installed and restart VS Code.
  - Run the command: `Metals: Import build`.

## Entry point
- Main class: `gov.va.occ.CDWPipe.Main`
