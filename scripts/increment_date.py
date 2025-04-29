from datetime import datetime, timedelta

file_paths = [
    "/scripts/date128.txt"
]

# Read the .txt file and increment it with 1
for file_path in file_paths:
    try:
        with open(file_path, "r") as f:
            current_date_str = f.read().strip()
        current_date = datetime.strptime(current_date_str, "%Y-%m-%d")
        next_date = current_date + timedelta(days=1)
        next_date_str = next_date.strftime("%Y-%m-%d")
        with open(file_path, "w") as f:
            f.write(next_date_str)

        print(f"Updated {file_path} with new date: {next_date_str}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
