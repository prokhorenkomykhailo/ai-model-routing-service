# Updated 80/20 Overlap Strategy Examples

## Overview
The sliding window overlap strategy now uses dynamic calculation based on the number of new messages:

## Rules:
1. **New messages ≥ 20**: Add **25% of new message count** for processed messages
2. **New messages < 5**: Add only **1 old message** (minimum overlap)
3. **New messages 5-19**: Calculate proportionally (new_messages / 4) to maintain 80/20 balance

## Examples:

### Large Batches (≥20 new messages)
- **20 new messages** → 5 old messages (25% of 20 = 5)
- **40 new messages** → 10 old messages (25% of 40 = 10)
- **80 new messages** → 20 old messages (25% of 80 = 20)
- **100 new messages** → 25 old messages (25% of 100 = 25)

### Small Batches (<5 new messages)
- **1 new message** → 1 old message (minimum)
- **2 new messages** → 1 old message (minimum)
- **4 new messages** → 1 old message (minimum)

### Medium Batches (5-19 new messages)
- **5 new messages** → 1 old message (5/4 = 1.25 → 1)
- **8 new messages** → 2 old messages (8/4 = 2)
- **12 new messages** → 3 old messages (12/4 = 3)
- **16 new messages** → 4 old messages (16/4 = 4)
- **19 new messages** → 4 old messages (19/4 = 4.75 → 4)

## Benefits:
- **Scalable**: Overlap grows proportionally with batch size for large batches
- **Efficient**: Minimal overlap for small batches
- **Consistent**: Maintains conversation context while avoiding excessive redundancy
- **Flexible**: Adapts to different workspace message volumes
