#!/usr/bin/env python3
"""Fix signal filters in token_api.py"""
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "/home/ec2-user/graphRAG/token_api.py"

with open(path, "r") as f:
    code = f.read()

changes = 0

# 1. Clean up shared_funder — remove redundant if/continue pattern
old1 = """        fb = n.get('funded_by')
        if fb in (None, 'none', ''):
            continue
        if fb:
            funder_groups[fb].append(n['address'])"""
new1 = """        fb = n.get('funded_by')
        if fb and fb not in ('none', ''):
            funder_groups[fb].append(n['address'])"""
if old1 in code:
    code = code.replace(old1, new1)
    print("1. shared_funder — cleaned up")
    changes += 1
else:
    print("1. shared_funder — already clean or not found")

# 2. Fix shared_fee_payer — add empty string check
old2 = """        fp = n.get("fee_payer")
        if fp and fp != "none":"""
new2 = """        fp = n.get("fee_payer")
        if fp and fp not in ("none", ""):"""
if old2 in code:
    code = code.replace(old2, new2)
    print("2. shared_fee_payer — fixed")
    changes += 1
else:
    print("2. shared_fee_payer — already clean or not found")

with open(path, "w") as f:
    f.write(code)

print(f"\n{changes} change(s) applied")
