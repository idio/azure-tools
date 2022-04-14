import subprocess

def cmd_output(s):
    s_rep = s
    if isinstance(s_rep, list):
        s_rep = " ".join(s_rep)

    print(f">> CMD OUTPUT: {s_rep}" + s_rep)
    return str(subprocess.check_output(s, shell=True))