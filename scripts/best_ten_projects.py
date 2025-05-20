import requests
import csv

# List of project accessions
project_ids = [
"PXD012988",
 "PXD007091",
 "PXD012987",
 "PXD012162",
 "PXD017812",
 "PXD004732",
 "PXD016999",
 "PXD013361",
 "PXD010154",
 "PXD017865"
]

# Output CSV file
output_file = "pride_projects_metadata.csv"

# Define CSV headers (fields we want to extract)
csv_headers = [
    "accession", "title",
    "projectDescription", "sampleProcessingProtocol", "dataProcessingProtocol", "instrumentNames",
    "species", "organisms", "organismParts", "diseases", "ptms", "keywords"
]

# Open CSV file for writing
with open(output_file, mode="w", newline='', encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=csv_headers)
    writer.writeheader()

    for project_id in project_ids:
        url = f"https://www.ebi.ac.uk/pride/ws/archive/v3/projects/{project_id}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()

            # Extract sample metadata
            instrument_names = "; ".join(instr.get("name", "") for instr in data.get("instruments", []))
            species = "; ".join(sp.get("name", "") for sp in data.get("species", []))
            organisms = "; ".join(sp.get("name", "") for sp in data.get("organisms", []))
            organismParts = "; ".join(sp.get("name", "") for sp in data.get("organismParts", []))
            diseases = "; ".join(ds.get("name", "") for ds in data.get("diseases", []))
            ptms = "; ".join(p.get("name", "") for p in data.get("ptms", []))
            keywords = "; ".join(data.get("keywords", []))

            # Prepare the row data
            row = {
                "accession": data.get("accession"),
                "title": data.get("title"),
                "projectDescription": data.get("projectDescription"),
                "sampleProcessingProtocol": data.get("sampleProcessingProtocol"),
                "dataProcessingProtocol": data.get("dataProcessingProtocol"),
                "instrumentNames": instrument_names,
                "species": species,
                "organisms": organisms,
                "organismParts": organismParts,
                "diseases": diseases,
                "ptms": ptms,
                "keywords": keywords
            }

            writer.writerow(row)
            print(f"Saved: {project_id}")
        else:
            print(f"Failed to fetch: {project_id} (HTTP {response.status_code})")

print(f"\nMetadata saved to: {output_file}")