# GitHub Setup Guide for Facebook Ad Scraper

This guide will help you push your Facebook Ad Scraper project to GitHub and configure it to run automatically using GitHub Actions.

## Step 1: Create a GitHub Repository

1. Go to [GitHub](https://github.com) and sign in to your account
2. Click the "+" icon in the top-right corner and select "New repository"
3. Name your repository (e.g., "facebook-ad-scraper")
4. Choose whether it should be public or private
5. Click "Create repository"

## Step 2: Prepare Your Local Repository

Open a terminal/command prompt in your project directory and run:

```bash
# Initialize git repository (if not already done)
git init

# Add all files to git
git add .

# Create a .gitignore file to exclude sensitive information
echo "credentials.json" >> .gitignore
echo ".env" >> .gitignore
echo "__pycache__/" >> .gitignore
echo "*.pyc" >> .gitignore
echo "logs/" >> .gitignore

# Commit your changes
git commit -m "Initial commit"

# Add your GitHub repository as the remote origin
git remote add origin https://github.com/YOUR_USERNAME/facebook-ad-scraper.git

# Push your code to GitHub
git push -u origin master
```

## Step 3: Set Up GitHub Secrets for Credentials

1. Go to your repository on GitHub
2. Click on "Settings" tab
3. In the left sidebar, click on "Secrets and variables" → "Actions"
4. Click "New repository secret"
5. Create a secret for your Google credentials:
   - Name: `GOOGLE_CREDENTIALS_JSON`
   - Value: *Copy the entire contents of your credentials.json file*
6. Click "Add secret"

## Step 4: Configure Your .env File

Keep your .env file locally with your API keys. This file should NEVER be committed to GitHub:

1. Create a local .env file in your project directory:

```
# .env file - KEEP THIS FILE LOCAL, DO NOT COMMIT TO GITHUB
CLAUDE_API_KEY=your_claude_api_key_here
```

## Step 5: Verify GitHub Actions Workflow

1. Go to the "Actions" tab in your GitHub repository
2. You should see the "Facebook Ads Scraper" workflow
3. The workflow is configured to run automatically at 4:00 PM Indian time (10:30 AM UTC)
4. You can also click "Run workflow" to trigger it manually

## Important Notes

1. **Column Names Preservation**: The script preserves column names with trailing spaces ('Page ', 'Page Transperancy ', 'No. of ads', and 'no.of ads By Ai')

2. **Error Logging**: Only errors are logged to the logs/fb_scraper_errors.log file

3. **Schedule**: The scraper runs daily at 4:00 PM Indian time (10:30 AM UTC)

4. **Credentials**: Never commit your credentials.json or .env files to GitHub

5. **Manual Run**: You can always run the workflow manually from the GitHub Actions tab
