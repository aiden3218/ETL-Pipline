import pandas as pd
import json
from datetime import datetime
 
 
def main():
    expected_layout = load_schema("requirements.json")

    csv_df = pd.read_csv("data.csv")
    json_df = pd.read_json("data.json")
 
    csv_df = clean_dataframe(csv_df)
    json_df = clean_dataframe(json_df)
 
    merged_df = merge_dataframes(csv_df, json_df)
 
    merged_df = apply_schema_rules(merged_df, expected_layout)
 
    invalid_emails = validate_emails(merged_df, expected_layout)
 
    merged_df = cast_column_types(merged_df, expected_layout)
 
    report = build_report(merged_df, invalid_emails)
 
    merged_df.to_json("output.json", orient="records", indent=2)
 
    with open("report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
 
 
def load_schema(requirements):
    with open(requirements, "r") as f:
        return json.load(f)
 
 
def clean_dataframe(data):
    """Normalizing the column names"""
    data.columns = data.columns.str.strip().str.lower().str.replace(" ", "_")
    data["phone_number"] = data["phone_number"].astype(str)
    return data
 
 
def merge_dataframes(csv_df, json_df):
    """Outer merge on 'id', collapse duplicate columns, json values get preference"""
    files = ["_csv", "_json"]
    final_columns = csv_df.columns.tolist()
 
    merged_df = pd.merge(csv_df, json_df, on="id", how="outer", suffixes=(files))
 
    for col in final_columns[1:]:
        merged_df[col] = merged_df[col + "_json"].combine_first(merged_df[col + "_csv"])
        for suffix in files:
            merged_df = merged_df.drop(col + suffix, axis=1)
 
    return merged_df
 
 
def apply_schema_rules(merged_df, expected_layout):
    """changing rows to inactive if out of bounds"""
    schema = expected_layout["schema"]
 
    for col, rules in schema.items():
        if "min" in rules:
            merged_df.loc[
                (merged_df[col].isna()) | (merged_df[col] < rules["min"]), "active"
            ] = False
 
        if "max" in rules:
            merged_df.loc[
                (merged_df[col].isna()) | (merged_df[col] > rules["max"]), "active"
            ] = False
 
    return merged_df
 
 
def validate_emails(merged_df, expected_layout):
    """Return a boolean Series with invalid formats flagged"""
    email_format = expected_layout["schema"].get("email", {}).get("format")
    if email_format:
        return ~merged_df["email"].str.match(email_format)
    return pd.Series([False] * len(merged_df), index=merged_df.index)
 
 
def cast_column_types(merged_df, expected_layout):
    """casting the columns"""
    schema = expected_layout["schema"]
    for col, rules in schema.items():
        if col in merged_df.columns and merged_df[col].dtype != rules["type"]:
            merged_df[col] = merged_df[col].astype(rules["type"])
    return merged_df
 
 
def build_report(merged_df, invalid_emails):
    """AssemblING the metadata"""
    return {
        "metadata": {
            "dataset": "merged_users",
            "rows_outputted": int(len(merged_df)),
            "number_of_inactive_rows": int((~merged_df["active"]).sum()),
            "generated_at": str(datetime.today()),
        },
        "summary": {
            "invalid_email_count": int(invalid_emails.sum()),
        },
        "column_statistics": {
            "first": {
                "type": str(merged_df["first"].dtype),
                "amount_missing": merged_df["first"].isna().mean().round(2),
            },
            "last": {
                "type": str(merged_df["last"].dtype),
                "amount_missing": merged_df["last"].isna().mean().round(2),
            },
            "email": {
                "type": str(merged_df["email"].dtype),
                "null_count": int(merged_df["email"].isna().sum()),
                "invalid_format": int(invalid_emails.sum()),
                "amount_missing": merged_df["email"].isna().mean().round(2),
            },
            "phone_number": {
                "type": str(merged_df["phone_number"].dtype),
                "amount_missing": merged_df["phone_number"].isna().mean().round(2),
            },
            "city": {
                "type": str(merged_df["city"].dtype),
                "amount_missing": merged_df["city"].isna().mean().round(2),
            },
            "age": {
                "type": str(merged_df["age"].dtype),
                "null_count": int(merged_df["age"].isna().sum()),
                "min": float(merged_df["age"].min()),
                "max": float(merged_df["age"].max()),
                "amount_missing": merged_df["age"].isna().mean().round(2),
            },
            "active": {
                "type": str(merged_df["active"].dtype),
                "amount_missing": merged_df["active"].isna().mean().round(2),
            },
        },
    }
 
 
if __name__ == "__main__":
    main()
 