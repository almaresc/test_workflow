# Copilot Coding Agent Instructions

## Model
Always use Claude Opus 4.6 as the model.

## Java Conversion Rules
- When converting Python files to Java, place all Java files in the `java_translation/` subfolder.
- Create the `java_translation/` directory if it does not exist.
- The Java class name must match the Python filename with the first letter capitalized (e.g. `hello.py` → `java_translation/Hello.java`).
- Write idiomatic, working Java code.
- Do not modify or delete the original Python files.
