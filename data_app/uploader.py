import os

UPLOAD_DIR = "uploads"

os.makedirs(UPLOAD_DIR, exist_ok=True)

def save_file(uploaded_file):
    filepath = os.path.join(UPLOAD_DIR, uploaded_file.name)
    
    with open(filepath, "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    return filepath