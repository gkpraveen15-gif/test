# Hands-on Lab: The Team Lead Simulation

Welcome to the practical portion! You are now the Team Lead. 
Alice and Bob have been pushing code, and it's time for you to aggregate the required features for the upcoming release.

## Step 1: Run the Simulation Setup
First, we need to generate our simulated team environment.
Open your terminal (ensure it's set to **Git Bash**) and run the setup script:
```bash
bash setup_team_simulation.sh
```
*(This script initializes the repo, creates files, and generates Alice and Bob's branches with specific commit histories).*

## Step 2: Survey the Landscape
Let's see what branches we have.
```bash
git branch -a
```
You should see:
- `main`
- `feature/alice-new-metrics`
- `feature/bob-data-fixes`

## Step 3: Create the Integration Branch
You are preparing the next release. Create a new branch off `main`.
```bash
git checkout main
git checkout -b release/v1.1
```

## Step 4: The Cherry-Pick (Rescuing Bob's Code)
Bob's branch (`feature/bob-data-fixes`) has a crucial fix, but he also committed some broken experimental code. We only want the fix.

1. Look at Bob's commits:
   ```bash
   git log feature/bob-data-fixes --oneline
   ```
2. Identify the hash (the short alphanumeric string) of the commit labeled **"fix(pipeline): correct data extraction logic"**.
3. **Cherry-pick** that specific commit to your release branch:
   ```bash
   git cherry-pick <commit-hash>
   ```
   *Notice how only the good code comes over, leaving the bad experimental code behind.*

## Step 5: The Merge (Integrating Alice's Code)
Alice did a great job. Her entire feature is reviewed and ready. We want to merge it, but we want to retain the context that this was a specific feature (by creating a merge commit).

1. Run the merge command:
   ```bash
   git merge feature/alice-new-metrics --no-ff -m "Merge feature: Alice's new metrics into release"
   ```
   *(The `--no-ff` ensures a merge commit is created, even if a fast-forward was possible).*

## Step 6: Review Your Masterpiece
Let's look at the beautiful, clean history you've created for the release:
```bash
git log --graph --oneline
```
You should see Bob's cherry-picked fix and a clean merge node representing Alice's feature.

## Step 7: Push and PR
In a real environment, you would now push this branch and raise a Pull Request!
```bash
# git push origin release/v1.1
```

**Congratulations!** You've just executed a Tier-1 Team Lead Git Workflow!
