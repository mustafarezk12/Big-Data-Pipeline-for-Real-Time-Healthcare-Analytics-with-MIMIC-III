from sys import stdin
for line in stdin:
    fields = line.strip().split(',')
    if fields[0] != 'row_id':  # Skip header
        dob = fields[3]  # DOB column
        try:
            age = 2025 - int(dob.split('-')[0])  # Current year - birth year
            print(f"{age}\t1")
        except:
            pass  