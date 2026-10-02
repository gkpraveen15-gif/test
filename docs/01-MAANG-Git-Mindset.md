# The MAANG Git Mindset for Data Engineers

At Tier-1 tech companies, Git isn't just a tool for saving code—it's a tool for **communication** and **deployment safety**. A messy Git history leads to broken data pipelines, difficult rollbacks, and confusion during incidents.

## 1. Linear History vs. Feature Merges
The eternal debate: Merge vs. Rebase. In top engineering teams, both are used, but for entirely different purposes:

- **Rebase (`git pull --rebase` or `git rebase main`)**: Use this when you are updating your *local feature branch* with the latest changes from `main`. Rebasing rewrites your local commits so they apply cleanly on top of the newest `main`. This prevents the dreaded "Merge branch 'main' into feature" spaghetti commits.
- **Merge (`git merge --no-ff`)**: Use this when bringing a *completed feature branch* into `main`. A non-fast-forward merge creates a specific "Merge Commit", which acts as a clear grouping for the feature. If the feature breaks the pipeline, the team can easily `git revert <merge-commit-hash>` to instantly roll back the entire feature.

## 2. Conventional Commits
Commit messages must explain the *why*, not just the *what*. We use conventional commits to make history readable and automatable (for generating release notes).

* **Bad**: `fixed sql query`
* **Good**: `fix(sql): correct window function partition in daily aggregates to prevent duplicate users`

Prefixes:
- `feat(component)`: A new feature (e.g., a new DAG).
- `fix(component)`: A bug fix (e.g., fixing a transformation).
- `chore(component)`: Maintenance (e.g., updating requirements.txt).

## 3. The Deployment Philosophy
When deploying Data Engineering pipelines, stability is king. You should never resolve conflicts *on the server* or *during the release*. All conflicts must be resolved locally on a feature branch before the PR is merged. The `main` branch must always be deployable.
