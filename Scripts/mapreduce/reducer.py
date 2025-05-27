import sys

def reduce_ages():
    total_age = 0.0
    count = 0

    try:
        for line in sys.stdin:
            key, age = line.strip().split('\t')
            total_age += float(age)
            count += 1

        if count > 0:
            average_age = total_age / count
            print(f"Average Patient Age: {average_age:.2f} years")
        else:
            print("No valid ages found", file=sys.stderr)
    except Exception as e:
        print(f"Error in reducer: {e}", file=sys.stderr)

if __name__ == "__main__":
    reduce_ages()