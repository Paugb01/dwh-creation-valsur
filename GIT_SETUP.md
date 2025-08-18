# Git Setup Instructions

## Install Git

### Option 1: Download from Official Website
1. Go to https://git-scm.com/download/win
2. Download and install Git for Windows
3. During installation, choose "Git from the command line and also from 3rd-party software"

### Option 2: Using Chocolatey (if installed)
```powershell
choco install git
```

### Option 3: Using Winget (Windows Package Manager)
```powershell
winget install --id Git.Git -e --source winget
```

## After Installing Git

1. **Configure Git** (replace with your information):
   ```bash
   git config --global user.name "Your Name"
   git config --global user.email "your.email@example.com"
   ```

2. **Initialize Repository**:
   ```bash
   git init
   ```

3. **Add files to staging**:
   ```bash
   git add .
   ```

4. **Create initial commit**:
   ```bash
   git commit -m "Initial commit: Working MySQL data extractor with PyMySQL"
   ```

5. **Optional: Connect to remote repository** (GitHub, GitLab, etc.):
   ```bash
   git remote add origin <repository-url>
   git branch -M main
   git push -u origin main
   ```

## Files Ready for Git

The following files are already configured for version control:
- ✅ `.gitignore` - Excludes unnecessary files
- ✅ `README.md` - Project documentation
- ✅ `requirements.txt` - Dependencies
- ✅ `simple_extractor.py` - Main code

## What's Excluded from Git (.gitignore)

- `.venv/` - Virtual environment
- `extracted_data/` - Output data files
- `*.log` - Log files
- `__pycache__/` - Python cache
- Credential files and temporary files

## Current Project Status

Ready for initial commit with:
- Working data extraction pipeline
- Clean project structure
- Proper documentation
- Version control configuration
