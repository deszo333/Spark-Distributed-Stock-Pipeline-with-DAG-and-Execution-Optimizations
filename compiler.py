import os

# ==============================
# CONFIG
# ==============================

ROOT_DIR = "./"  # your project root
OUTPUT_FILE = "compiled_project.txt"

# File types to include
INCLUDE_EXTENSIONS = {".py", ".jsx", ".js"}

# Folders to ignore
EXCLUDE_DIRS = {
    "node_modules",
    ".git",
    "__pycache__",
    "dist",
    "build",
    ".venv",
    "venv"
}

# Files to ignore
EXCLUDE_FILES = {
    "package-lock.json",
    "yarn.lock"
}

# ==============================
# HELPERS
# ==============================

def is_valid_file(file_path):
    _, ext = os.path.splitext(file_path)
    return ext in INCLUDE_EXTENSIONS


def should_skip_dir(dirname):
    return dirname in EXCLUDE_DIRS


def should_skip_file(filename):
    return filename in EXCLUDE_FILES


# ==============================
# BUILD TREE STRUCTURE
# ==============================

def get_folder_structure(root_dir):
    structure = []

    for root, dirs, files in os.walk(root_dir):
        # filter dirs
        dirs[:] = [d for d in dirs if not should_skip_dir(d)]

        level = root.replace(root_dir, "").count(os.sep)
        indent = "  " * level
        structure.append(f"{indent}{os.path.basename(root)}/")

        sub_indent = "  " * (level + 1)
        for f in files:
            if is_valid_file(f) and not should_skip_file(f):
                structure.append(f"{sub_indent}{f}")

    return "\n".join(structure)


# ==============================
# READ FILE CONTENTS
# ==============================

def read_files(root_dir):
    compiled = []

    for root, dirs, files in os.walk(root_dir):
        dirs[:] = [d for d in dirs if not should_skip_dir(d)]

        for file in files:
            if should_skip_file(file):
                continue

            file_path = os.path.join(root, file)

            if not is_valid_file(file_path):
                continue

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                compiled.append(f"\n\n{'='*80}")
                compiled.append(f"FILE: {file_path}")
                compiled.append(f"{'='*80}\n")
                compiled.append(content)

            except Exception as e:
                compiled.append(f"\n[ERROR READING {file_path}]: {e}")

    return "\n".join(compiled)


# ==============================
# MAIN
# ==============================

def compile_project():
    print("📦 Scanning project...")

    structure = get_folder_structure(ROOT_DIR)
    files_content = read_files(ROOT_DIR)

    final_output = f"""
==============================
📁 PROJECT STRUCTURE
==============================
{structure}

==============================
📄 FILE CONTENTS
==============================
{files_content}
"""

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(final_output)

    print(f"Compilation complete → {OUTPUT_FILE}")


if __name__ == "__main__":
    compile_project()