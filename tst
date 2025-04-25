import os
import shutil
import pandas as pd


def copy_sql_scripts_from_list(excel_path, source_dir, target_dir, summary_output="copy_status_summary.xlsx"):
    df = pd.read_excel(excel_path)
    script_names = df['Script_Name'].dropna().tolist()

    os.makedirs(target_dir, exist_ok=True)

    status_list = []

    for script_name in script_names:
        try:
            found = False
            for root, _, files in os.walk(source_dir):
                if script_name in files:
                    src_path = os.path.join(root, script_name)
                    dest_path = os.path.join(target_dir, script_name)
                    shutil.copy(src_path, dest_path)

                    found = True
                    break

            if not found:
                print(f"❌ Not found: {script_name}")
                status_list.append({"Script_Name": script_name, "Status": "Not Found", "Error": "File not found"})
        except Exception as e:
            print(f"⚠️ Error copying {script_name}: {e}")
            status_list.append({"Script_Name": script_name, "Status": "Error", "Error": str(e)})

        if not found:
            print(f"❌ Not found: {script_name}")
            status_list.append({"Script_Name": script_name, "Status": "Not Found", "Error": "File not found"})
            print(f"✅ Copied: {script_name}")
            status_list.append({"Script_Name": script_name, "Status": "Copied", "Error": ""})
        # handled by recursive search
        except Exception as e:
        print(f"⚠️ Error copying {script_name}: {e}")
        status_list.append({"Script_Name": script_name, "Status": "Error", "Error": str(e)})


summary_df = pd.DataFrame(status_list)
summary_df.to_excel(summary_output, index=False)
print(f"\n📋 Summary report saved to: {summary_output}")

# Example usage
if __name__ == "__main__":
    excel_path = "script_list.xlsx"
    source_dir = "C:/Your/Full/SQL_Project_Path"
    target_dir = "C:/Your/Destination/min_sql_scripts"
    copy_sql_scripts_from_list(excel_path, source_dir, target_dir)
