# Maharashtra Education Dashboard - Local Hosting Guide

## 🌐 Live Demo (GitHub Pages)

- **Frontend UI**: [MAHA Education Dashboard (Demo)](https://oha2025g.github.io/MAHA-Education-Dashboard/)
- Note: GitHub Pages hosts the **frontend only**. For real data + login, run **MongoDB + backend** locally (instructions below).

## 📋 Prerequisites

Before you begin, ensure you have the following installed:

1. **Node.js** (v16 or higher) - [Download](https://nodejs.org/)
2. **Python** (v3.9 or higher) - [Download](https://python.org/)
3. **MongoDB** (v5.0 or higher) - [Download](https://www.mongodb.com/try/download/community)
4. **Yarn** (Package manager) - Install with: `npm install -g yarn`

## 🚀 Quick Start

### Step 1: Extract the Package
```bash
unzip maharashtra-edu-dashboard-local.zip
cd maharashtra-edu-dashboard-local
```

### Step 2: Start MongoDB
Make sure MongoDB is running on your system:
```bash
# On Linux/Mac
sudo systemctl start mongod
# OR
mongod --dbpath /path/to/data/db

# On Windows (run as Administrator)
net start MongoDB
```

### Step 3: Import the Database
```bash
mongorestore --db maharashtra_edu data/mongodb/maharashtra_edu/
```

### Step 4: Setup Backend

```bash
cd backend

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create .env file
cp .env.template .env
# Edit .env if needed (default values work for local setup)

# (Optional) Enable AI Insights (ChatGPT / OpenAI)
# Create backend/.env.local and set:
#   OPENAI_API_KEY=...
#   OPENAI_MODEL=gpt-4o-mini
# See: backend/env.local.template

# Start the backend server
uvicorn server:app --host 0.0.0.0 --port 8001 --reload
```

The backend will be available at: http://localhost:8001

### Step 5: Setup Frontend (in a new terminal)

```bash
cd frontend

# Create .env file
cp .env.template .env

# Install dependencies
yarn install

# Start the development server
yarn start
```

The frontend will be available at: http://localhost:3000

## 🔑 Default Login Credentials

```
Email: admin@mahaedume.gov.in
Password: admin123
```

## 📊 What's Included

### Data Files (10 Excel files with Pune District data)
| File | Description |
|------|-------------|
| 01_Aadhaar_Status.xlsx | Aadhaar verification status by school |
| 02_School_Comparison.xlsx | School-wise comparison metrics |
| 03_Water_Infrastructure.xlsx | Drinking water and infrastructure |
| 04_Enrolment_ClassWise.xlsx | Class-wise student enrolment |
| 05_Dropbox_Remarks.xlsx | Dropbox remarks statistics |
| 06_Data_Entry_Status.xlsx | Data entry completion status |
| 07_Age_Wise_Enrolment.xlsx | Age-wise student distribution |
| 08_CTTeacher_Data.xlsx | Teacher data and qualifications |
| 09_APAAR_Entry_Status.xlsx | APAAR ID generation status |
| 10_Classrooms_Toilets.xlsx | Classrooms and toilet facilities |

### Database Collections
- `aadhaar_analytics` - 7,384 school records
- `apaar_analytics` - 11,639 records
- `teacher_analytics` - 7,385 records
- `infrastructure_analytics` - 7,385 records
- `enrolment_analytics` - 7,384 records
- `dropbox_analytics` - 8,028 records
- `data_entry_analytics` - 7,384 records
- `age_enrolment` - 57,076 records
- `ctteacher_analytics` - 77,389 teacher records
- `classrooms_toilets` - 7,385 records
- `users` - Admin user

### Dashboards Available
1. **Executive Dashboard** - Overview with SHI metrics + Maharashtra Map
2. **State Overview** - State-level summary
3. **Aadhaar Analytics** - Aadhaar verification status
4. **APAAR Entry Status** - APAAR ID generation tracking
5. **Enrolment Analytics** - Student enrolment analysis
6. **Age-wise Enrolment** - Age distribution analysis
7. **Teacher Analytics** - Teacher deployment and training
8. **CTTeacher Analytics** - Detailed teacher data analysis
9. **Infrastructure & Water Safety** - School infrastructure
10. **Classrooms & Toilets** - Facilities analysis
11. **Data Entry Status** - Data completion tracking
12. **Dropbox Remarks** - Remarks and issues tracking
13. **Advanced Analytics** - AI-powered insights (requires OpenAI API key)
14. **School Health Index** - Composite health scoring

## 🗺️ Maharashtra Map

The Executive Dashboard includes an interactive choropleth map of Maharashtra:
- Currently shows data for Pune district only
- 35 other districts shown as "No Data"
- Supports multiple metrics: SHI, Aadhaar %, APAAR %, Infrastructure Index, CTET %

## 🔄 Re-running ETL Pipeline (Optional)

If you want to reload data from Excel files:

```bash
cd backend
python -c "from etl.etl_pipeline import run_etl; import asyncio; asyncio.run(run_etl())"
```

## 📝 Adding More District Data

To add data for other Maharashtra districts:
1. Place Excel files in `backend/uploads/` folder
2. Run the ETL pipeline (see above)
3. The map will automatically show the new districts

## 🛠️ Troubleshooting

### MongoDB Connection Error
- Ensure MongoDB is running: `sudo systemctl status mongod`
- Check the MONGO_URL in backend/.env

### Port Already in Use
- Backend: Change port in uvicorn command
- Frontend: Use `PORT=3001 yarn start`

### Module Not Found
- Backend: Ensure virtual environment is activated
- Frontend: Run `yarn install` again

## 📧 Support

For issues or questions, please refer to the PRD.md file or raise an issue in the repository.

---
Built with ❤️ for Maharashtra Education Department
