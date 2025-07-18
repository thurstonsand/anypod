# CLAUDE.md

## Testing Guidelines

### Test Structure and Organization
- Unit tests should mirror source structure (found in `src/anypod`) in `/tests/anypod/`
- Integration tests go in `/tests/integration/` with `integration_test_` prefix - run with `--integration` flag
- Add `# pyright: reportPrivateUsage=false` to test files for protected method access
- Use pytest markers: either `@pytest.mark.unit` or `@pytest.mark.integration` for all tests
- Import packages like `from anypod.db import Database`, not `from src.anypod`
- When adding new tests to an existing file:
  - function/method tests should be ordered in the same order as the actual code file
  - if there are already tests for the function you are adding a test for, add the new test next to the others

### Test Writing Patterns
- Use Arrange-Act-Assert pattern without explicit comments
- Descriptive test names that describe behavior
- One logical assertion per test when possible
- Test both success and failure paths
- Add descriptive messages to non-obvious assertions
- If a constant is used in multiple places, consolidate it into a variable and reference it instead of copy-pasting it everywhere
- Do not assert check for the string content of an exception, as this is fragile

### Test Execution
```bash
# Unit tests only
uv run pytest --no-cov

# Include integration tests, expensive and makes network calls
uv run pytest --integration --no-cov

# With coverage
uv run pytest --cov-report=xml
```

### Key Testing Rules
- Mock at appropriate levels using `pytest-mock`
- Don't mock external libraries you don't own
- Avoid string checks on error messages
- Keep tests fast and independent
- Aim for meaningful coverage, not 100% blindly
- Include security tests for input validation when testing HTTP endpoints
- Perform a full confirmation run (`uv run pytest --integration`) to ensure all tests pass after you finish writing and verifying.