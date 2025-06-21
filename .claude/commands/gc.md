`git commit` the currently staged code; additional context: $ARGUMENTS

- `pre-commit` hooks will trigger on commit. If they error, the commit will fail; address any issues and try the commit again
- Do not stage any additional files
- Unless the commit specifically and only concerns testing, keep comments about test files concise, at most to one line