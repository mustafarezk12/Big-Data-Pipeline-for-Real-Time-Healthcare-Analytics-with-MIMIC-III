import sys

def reduce_ages():
    total_age = 0
    count = 0

    for line in sys.stdin:
        _, age = line.strip().split('\t')
        total_age += float(age)
        count += 1

    if count > 0:
        average_age = total_age / count
        print(f"Average Patient Age: {average_age:.2f} years")

if __name__ == "__main__":
    reduce_ages()
