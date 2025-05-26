from sys import stdin
total_age, count = 0, 0
for line in stdin:
    try:
        age, one = map(int, line.strip().split('\t'))
        total_age += age
        count += one
    except:
        pass  # Skip invalid rows
if count > 0:
    print(f"Average Age: {total_age / count}")
else:
    print("No valid data")