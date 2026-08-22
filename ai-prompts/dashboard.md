# AI Prompts — Data Generation
## Prompt 1: Initial Data Generation Script
**PROMPT SENT:**
"Generate Python script to create realistic e-commerce customer data.
I need 10,000 rows with these fields: customer_id (INT), customer_name (STRING),
email (STRING), country (STRING), signup_date (DATE between 2020-2024),
customer_segment (Premium/Standard/Basic), lifetime_value (DECIMAL).
Include realistic values like actual names, valid email formats, and random
dates."
**AI RESPONSE SUMMARY:**
[Cursor generated Python script using faker library to create realistic data]
**YOUR EVALUATION:**
✓ **What was good:**
- Used faker for realistic names and emails
- Date range correct (2020-2026)
- Customer segments randomized properly
✗ **What needed fixing:**
- Some customers had signup_date in future
- No intentional quality issues (needed 50 NULL emails, 10 duplicates, etc.)
- Missing lifetime_value calculations
△ **Missing:**
- No NULL values as needed for quality testing
## Iteration 1: Adding Quality Issues
**PROMPT SENT:**
"Modify the script to introduce intentional quality issues for testing:
- 50 rows with NULL email
- 10 rows with duplicate customer_id
- 30 rows with signup_date > today()
Keep the rest realistic. Add comments explaining the quality issues."
**AI RESPONSE SUMMARY:**
[Cursor modified script to add quality issues and comments]
**YOUR EVALUATION:**
✓ **ACCEPTED** - Modifications correct, quality issues intentional and commented

**FINAL DECISION:** Use this version as `generate_sample_data.py`
---
## Prompt 2: Order Data Generation
**PROMPT SENT:**
"Generate Python script for 100,000 realistic e-commerce order rows...
[similar structure for orders]"
**[Continue pattern for each major prompt]**