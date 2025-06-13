import csv
import random
import os
from faker import Faker
from multiprocessing import Process, cpu_count

fake = Faker()

output_file = "student_dataset.csv"
temp_dir = "temp_chunks"
os.makedirs(temp_dir, exist_ok=True)

target_size_bytes = 1 * 1024 ** 3  # 1 GB in bytes

years = list(range(2011, 2026))
majors = ['Computer Science', 'Physics', 'Mathematics', 'Chemistry', 'Biology',
          'Engineering', 'Economics', 'Business', 'Psychology', 'Sociology']
genders = ['Male', 'Female', 'Other']
subjects = ['Math', 'Physics', 'Chemistry', 'English', 'Biology', 'Economics', 'History', 'Art']

num_processes = cpu_count()

def generate_row(student_id):
    name = fake.name()
    gender = random.choice(genders)
    year = random.choice(years)
    major = random.choice(majors)
    subject = random.choice(subjects)
    marks = random.randint(0, 100)
    
    # Calculate age roughly: assume student entered school at age 6 + years since that
    current_year = 2025
    age = current_year - year + 6 + random.randint(-1, 1)  # add small randomness
    
    attendance_percentage = round(random.uniform(50, 100), 2)
    passed = "Yes" if marks >= 50 else "No"

    return [student_id, name, gender, year, major, subject, marks, age, attendance_percentage, passed]

def worker(proc_id, start_id):
    filename = os.path.join(temp_dir, f"chunk_{proc_id}.csv")
    with open(filename, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['student_id', 'name', 'gender', 'year', 'major', 'subject', 'marks',
                         'age', 'attendance_percentage', 'passed'])
        i = start_id
        while True:
            student_id = f"S{i:07d}"
            row = generate_row(student_id)
            writer.writerow(row)
            i += 1

            f.flush()
            os.fsync(f.fileno())

            current_size = os.path.getsize(filename)
            if current_size >= target_size_bytes / num_processes:
                break

    print(f"Process {proc_id} finished writing {i - start_id} rows.")

def merge_chunks():
    with open(output_file, mode='w', newline='', encoding='utf-8') as out_f:
        writer = csv.writer(out_f)
        writer.writerow(['student_id', 'name', 'gender', 'year', 'major', 'subject', 'marks',
                         'age', 'attendance_percentage', 'passed'])

        for proc_id in range(num_processes):
            filename = os.path.join(temp_dir, f"chunk_{proc_id}.csv")
            with open(filename, mode='r', encoding='utf-8') as in_f:
                reader = csv.reader(in_f)
                next(reader)  # skip header in chunk
                for row in reader:
                    writer.writerow(row)
            os.remove(filename)

def main():
    processes = []

    for proc_id in range(num_processes):
        start = proc_id * 10_000_000
        p = Process(target=worker, args=(proc_id, start))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    print("Merging chunk files...")
    merge_chunks()
    os.rmdir(temp_dir)

    size_gb = os.path.getsize(output_file) / (1024 ** 3)
    print(f"Dataset generated: {output_file} ({size_gb:.2f} GB)")

if __name__ == "__main__":
    main()
