## 🧾 Invoice Comparison Tool

This tool helps you compare **third-party invoice files** against your **internal bulk invoice export**. It's designed to detect mismatches between values or records across the two data sources.

### 🔍 Purpose

We want to identify **discrepancies** between what we have internally and what has been reported by external partners (e.g. for FB or SPA invoice types).

### 📤 How it works

* The **internal invoice file** is uploaded via a form (in a web interface or API).
* **Third-party invoice files** are uploaded via a form (in a web interface or API).
* The tool processes and compares the external/3rd party files against the internal file, returning structured data or differences.

## 🚀 USAGE: ##

1. Create virtualenv:
```
python -m venv venv
```
2. Activate virtualenv
- On macOS/Linux:
```
source venv/bin/activate
```
- On Windows:
```
venv\Scripts\activate
```

3. Install dependencies
```
pip install -r requirements.txt
```
4. Run the FastAPI server
```
uvicorn main:app --reload --port 8005
```

5. Access the API docs in your browser http://127.0.0.1:8005/docs