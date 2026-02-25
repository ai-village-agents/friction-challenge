import struct
import io

def generate_corrupted_file(filename):
    """Generates a file with silent corruption (NULL bytes, zero-width chars)."""
    with open(filename, "w", encoding="utf-8") as f:
        f.write("Header: Valid Data\n")
        f.write("Row 1: 100, 200, 300\n")
        f.write("Row 2: 400, 5\u200b00, 600\n") # Zero-width space in 500
        f.write("Row 3: 700, \x00800, 900\n") # NULL byte in 800
        f.write("Footer: End of File\n")

def process_file(filename):
    """Processes the file, detecting and fixing corruption."""
    print(f"Processing {filename}...")
    cleaned_data = []
    
    with open(filename, "rb") as f:
        content = f.read()
        
    # Detect NULL bytes
    if b'\x00' in content:
        print("WARNING: NULL bytes detected! Cleaning...")
        content = content.replace(b'\x00', b'')
        
    text_content = content.decode("utf-8")
    
    # Detect Zero-width characters
    if '\u200b' in text_content:
        print("WARNING: Zero-width characters detected! Cleaning...")
        text_content = text_content.replace('\u200b', '')
        
    for line in text_content.splitlines():
        if "Row" in line:
            parts = line.split(":")[1].split(",")
            cleaned_row = [int(p.strip()) for p in parts]
            cleaned_data.append(cleaned_row)
            print(f"Cleaned Row: {cleaned_row}")
            
    return cleaned_data

if __name__ == "__main__":
    filename = "corrupted_data.txt"
    generate_corrupted_file(filename)
    data = process_file(filename)
    print("Final Data:", data)
