import pandas as pd
import json
from datetime import datetime



def main():
    #reading requirments from .json file
    expected_layout = load_schema("requirements.json")
    

    csv_df = pd.read_csv("data.csv")
    json_df = pd.read_json("data.json")


    #cleaning data columns; need phone number as string and not int
    csv_df.columns = csv_df.columns.str.strip().str.lower().str.replace(" ", "_")
    json_df.columns = json_df.columns.str.strip().str.lower().str.replace(" ", "_")
    json_df["phone_number"] = json_df["phone_number"].astype(str)
    csv_df["phone_number"] = csv_df["phone_number"].astype(str)

    #creating merges by csv file
    final_columns = csv_df.columns.tolist()
    files = ['_csv','_json']

    #merging
    merged_df = pd.merge(csv_df, json_df, on="id", how="outer", suffixes=(files))

    #removing id column from loop as it is the merging column
    final_columns.pop(0)

    #looping through each column and merging duplicate columns
    for i in final_columns:
        #removing duplicate columns
        merged_df[i] = merged_df[i + "_json"].combine_first(merged_df[i + "_csv"])
        for j in files:
            merged_df = merged_df.drop(i + j, axis=1)
        

    #iterating through requirements
    for i in final_columns:
        if "min" in expected_layout["schema"][i]:
            merged_df.loc[(merged_df[i].isna()) | (merged_df[i] < expected_layout["schema"][i]["min"]),"active"] = False
        
        if "max" in expected_layout["schema"][i]:
            merged_df.loc[(merged_df[i].isna()) | (merged_df[i] > expected_layout["schema"][i]["max"]),"active"] = False

        if "format" in expected_layout["schema"][i]:
            invalid_emails = ~merged_df["email"].str.match(expected_layout["schema"][i]["format"])


    #setting proper column types
    for i in final_columns:
        if merged_df[i].dtype != expected_layout["schema"][i]["type"]:
            merged_df[i] = merged_df[i].astype(expected_layout["schema"][i]["type"])




    report = {
        "metadata": {
            "dataset": "merged_users",
            "rows_outputted": int(len(merged_df)),
            "number_of_inactive_rows": int((~merged_df["active"]).sum()),
            "generated_at": str(datetime.today())
        },

        "summary": {
            "invalid_email_count": int(invalid_emails.sum()),
        },

        "column_statistics": {
            "first": {
                "type": str(merged_df["first"].dtype),
                "amount_missing": merged_df["first"].isna().mean().round(2)
            },
            "last": {
                "type": str(merged_df["last"].dtype),
                "amount_missing": merged_df["last"].isna().mean().round(2)
            },
            "email": {
                "type": str(merged_df["email"].dtype),
                "null_count": int(merged_df["email"].isna().sum()),
                "invalid_format": int(invalid_emails.sum()),
                "amount_missing": merged_df["email"].isna().mean().round(2)
            },
            "phone_number": {
                "type": str(merged_df["phone_number"].dtype),
                "amount_missing": merged_df["phone_number"].isna().mean().round(2)
            },
            "city": {
                "type": str(merged_df["city"].dtype),
                "amount_missing": merged_df["city"].isna().mean().round(2)
            },
            "age": {
                "type": str(merged_df["age"].dtype),
                "null_count": int(merged_df["age"].isna().sum()),
                "min": float(merged_df["age"].min()),
                "max": float(merged_df["age"].max()),
                "amount_missing": merged_df["city"].isna().mean().round(2)
            },
            "active": {
                "type": str(merged_df["active"].dtype),
                "amount_missing": merged_df["city"].isna().mean().round(2)
            }
        }

    }
    merged_df.to_json("output.json",orient="records",indent=2)

    with open("report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)


def load_schema(requirements):
    with open(requirements, "r") as f:
        return json.load(f)
    
def cleaning(data):
    data.columns = data.columns.str.strip().str.lower().str.replace(" ", "_")
    data["phone_number"] = data["phone_number"].astype(str)
    return data


if __name__ == "__main__":
    main()
