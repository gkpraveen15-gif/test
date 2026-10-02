# The Team Lead Playbook: Aggregating & Reviewing Code

As a Team Lead or Senior Data Engineer, you often have to aggregate work from multiple team members, salvage useful code from messy branches, and ensure the final PR is pristine.

## 1. The Cherry-Pick (`git cherry-pick`)
Sometimes a team member commits a brilliant hotfix alongside broken experimental code on the same branch. You don't want to merge their whole branch. You want the fix.

`git cherry-pick <commit-hash>` allows you to pluck a specific commit from anywhere in the repository and apply it to your current branch.

### The Cherry-Pick Workflow:
1. **Find the commit**: `git log --oneline feature/bob-broken-branch`
2. **Checkout your target branch**: `git checkout release/v1.0`
3. **Pick the commit**: `git cherry-pick a1b2c3d`
4. *(If there's a conflict)*: Open the conflicting file, resolve it, then run:
   - `git add <resolved-file>`
   - `git cherry-pick --continue`

## 2. Managing Multiple Feature Branches
When integrating work from Alice and Bob for a combined release:
1. Create a release branch: `git checkout -b release/next-sprint main`
2. Bring in Alice's complete, reviewed feature: `git merge feature/alice-dag --no-ff`
3. Bring in Bob's specific bug fix: `git cherry-pick <bobs-fix-hash>`
4. Push the clean, aggregated release branch and raise a PR.

## 3. Squashing Messy Histories
If a junior engineer has 15 commits like "wip", "typo", "fix typo again", do not merge that into main.
Use Interactive Rebase to squash them:
`git rebase -i HEAD~15`
Change `pick` to `squash` (or `s`) for all commits except the first one. This bundles them into one clean, atomic commit.
