# Background

This file defines the rules that AI agents must follow whenever working in this repository.
Its purpose is to provide concise, consistent decision criteria while maintaining quality, maintainability, and safety.

## General Principles

### Quality and Design

- Always consider quality, maintainability, and safety, as well as functionality.
- Choose an appropriate quality level for the current phase: prototype, MVP, or production.
- Do not leave issues unaddressed; fix them or at least document them explicitly.
- Follow the Boy Scout Rule: leave the areas you touch in a better state.
- Follow DRY and maintain a single source of truth.
- Use names that convey intent and keep the style consistent across the project.
- Fix even minor inconsistencies.
- Use comments to explain why; express what the code does through the code itself.
- Consider improvements to existing code when adding features.
- Break large changes into small steps.
- Actively remove unused code.
- Document technical debt in comments or documentation.
- Apply YAGNI: do not turn hypothetical use cases into requirements or add settings or branches for them; implement only what the current request and existing behavior require.
- Prioritize readability and add brief explanations only for code that is difficult to understand.
- Do not use ternary operators.
- Do not add defensive implementations.

### Error Handling and Reliability

- Resolve errors even when they appear only loosely related to the task.
- Do not swallow exceptions; fix their root causes.
- Detect errors early and provide clear messages.
- Include error cases in tests.
- Design external API and network interactions with failure in mind.
- Configure appropriate timeouts.
- Consider retries, including exponential backoff, when needed.
- Use circuit breakers when needed.
- Design for resilience to transient failures.
- Ensure observability through appropriate logging and metrics.

### Testing and Verification

- Do not skip tests; identify and fix the causes of problems.
- Test behavior rather than implementation details.
- Eliminate dependencies between tests so they can run in any order.
- Keep tests fast and reproducible.
- Treat coverage as a metric and prioritize test quality.
- Resolve errors and build failures until the checks succeed.

### Security, Performance, and Dependencies

- Manage secrets through environment variables; do not hardcode them.
- Validate all external input.
- Operate with the minimum required privileges.
- Do not add unnecessary dependencies.
- Check licensing, size, and maintenance status before adding dependencies.
- Update dependencies regularly for security and bug fixes.
- Run security audit tools regularly.
- Base optimizations on measurements, not assumptions.
- Consider extensibility from the early stages.
- Defer loading resources until they are needed.
- Define cache lifetimes and invalidation strategies clearly.
- Avoid N+1 queries and overfetching.

### Development Practices

- Balance business and technical requirements.
- Maintain minimum quality standards even under time constraints.
- Choose implementations appropriate for the team's technical skills.
- Choose the best solution within the constraints, recognizing that perfection is not always possible.
- Prioritize simplicity in prototypes and robustness in production.
- Document tradeoffs and their reasons.
- Use Conventional Commits; see `.gitmessage.txt`.
- Keep commits atomic and focused on a single change.
- Write clear commit messages in English.
- Do not commit directly to `main`.
- Review code rather than people, and treat feedback as constructive suggestions.
- Explain the reasons for changes and their impact clearly.
- Treat feedback as an opportunity to learn.
- Base debugging on reproducible steps, bisection, and inspection of recent changes.
- Use debuggers and profilers when needed.
- Record findings and solutions so they can be reused.
- Clearly describe the overview, setup, and usage in the README.
- Keep documentation aligned with code changes.
- Prefer practical examples.
- Record important design decisions in ADRs.
- Apply lessons learned to subsequent work and improve regularly.
- Evaluate new tools and techniques appropriately before adopting them.
- Document knowledge for the team and future developers.

## Prohibitions

- Do not delete files or directories without permission.
- Access only files and directories within the repository scope containing this `AGENTS.md`.
- Do not use `setAccessible` in test code.
- Do not test private methods directly. Aim for 100% C0 / C1 through public APIs.
- If public APIs cannot provide the required coverage, reconsider the production code structure.

## Project-Specific Rules

### Communication

- Communicate with the user in Japanese.
- Write project artifacts, including comments, JavaDoc, README files, AGENTS.md, commit messages, and PR descriptions, in English.

### Implementation Conventions

- Follow Java 11 and Spring Boot 2.7.18 requirements.
- Use the OSS libraries already defined in `pom.xml`.
- Do not leave unused fields, local variables, or imports after making changes.
- Complete the work only when there are zero unused warnings, including `The value of the field xxx is not used`.

### Testing and Builds

- Before running tests, check prior results from the conversation, user reports, local logs / reports, or CI, and identify any subsequent changes that affect the tested behavior.
- Reuse successful results, including measured C0 / C1, when the current changes and relevant test conditions have already been validated. A new turn or missing results in the current conversation alone is not a reason to rerun tests.
- Run `mvn clean test` for unvalidated logic or build / test configuration changes. Rerun only when subsequent changes, failures, or a specific unresolved concern justify it, or when explicitly requested by the user.
- Do not run tests solely to create a branch, stage changes, or commit already validated changes. Skip tests for documentation-only changes and formatting-only changes that do not affect behavior or test code.
- Before running tests, explicitly classify the changes as logic changes, configuration changes, or formatting-only changes.
- Before running tests, state the reason in one line. Do not run tests if you cannot state a reason.
- If tests or builds fail, identify the cause and resolve it until they succeed.
- If C0 or C1 is below 100%, add the necessary test cases.
- For any task that runs `mvn clean test`, include measured C0 / C1 (INSTRUCTION / BRANCH) in the completion report, with both percentages and covered / total counts.
- Do not mark the task complete without reporting those measurements.
- If tests fail during work that adds, modifies, or removes code, analyze and resolve the failures.
- Use JUnit 5 and mockit.

### Test Method Naming

- Preserve the existing Japanese test method naming convention: `<methodName>_<caseType><caseSuffix>_<verification>_<expectedResult>`. The case suffix is the Japanese word for "case".
- Use the Japanese labels for "normal" or "abnormal" as the case type.
- End every expected-result segment with the Japanese phrase romanized as "de aru koto". Romanization here explains the rule; continue writing these segments in Japanese script.
- Express the specific conditions concisely in the verification segment using an action verb.
- End the expected-result segment with a noun or an outcome expression before the required ending.
- Avoid wording equivalent to "is in a state of doing". Prefer outcome expressions such as "is returned", "is set", or "is rethrown", nominalized in Japanese.
- Where possible, avoid nominalizing the action in the verification segment; put any necessary explanation in test comments.

### Releases

- Match release tags to the `<version>` in `pom.xml`.
- Always inspect `pom.xml` in the diff before creating a tag.
- Create a tag only after every job in `ci.yml` has succeeded for the target commit.
- Do not create tags while CI is failing or still running.
- Verify 100% C0 / C1 (INSTRUCTION / BRANCH) in `jacoco.xml` or CI logs. Do not release if either is below 100%.
- After pushing `main`, create an annotated tag and push it to trigger `release.yml`.
- A release is complete only after confirming all three conditions: `release.yml` succeeds, the GitHub Release exists, and all expected assets are registered.

### JavaDoc

- Write all JavaDoc using the `/** ... */` format.
- Add a concise description to every method, including private methods, except in test code.
- Use `@param` and `@return` to describe parameters and return values, including their specific meaning and conditions.
- Document specifications, decision criteria, exceptions, and side effects in the body or with `@throws` when clarification is needed.

## Improving This File

- The growing volume of rules currently increases the context burden.
- Preserve the content while prioritizing less duplication, clearer organization, and consistent wording in future updates.
