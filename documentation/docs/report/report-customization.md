# Customizations

## Overview
The File Download Statistics report is designed to provide insights into project-level, trends, regional, and user-level statistics. This report is generated based on a predefined HTML template. However, users can customize the report according to their resource requirements by creating their own HTML template.

## Customizing the Report
You can modify the structure, styling, and content of the report by designing a custom HTML template. The placeholders in the template (e.g., `{{project_level_content}}`, `{{trends_content}}`, `{{maps_content}}`, `{{user_content}}`) will be dynamically populated with relevant data.

### Steps to Customize:
1. **Create a New HTML Template**
   - Copy the existing template.
   - Modify the HTML structure as per your requirements.
   - Ensure the required placeholders are maintained for dynamic content population.
   
2. **Save the Custom Template**
   - Name the template according to your preference (e.g., `my_resource_report.html`).

3. **Specify the Template in Configuration**
   - In your YAML configuration file, update the `report_template` parameter:
     
     ```yaml
     report_template: "<resource_name>_report.html"
     ```
   - Replace `<resource_name>` with the actual name of your custom report template.

## Example HTML Template
Below is the default HTML template that can be used as a reference:

```html
<!DOCTYPE html>
<html>
<head>
    <link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/materialize/0.100.2/css/materialize.min.css">
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
</head>
<body>
    <div class="container">
        <div class="center">
            <h1 class="black center" style="color: white; padding-top: 15px; padding-bottom: 15px; border-bottom: 4px solid green">
                File Download Statistics
            </h1>
        </div>
        <div class="content">
            <h4 style="border-bottom: 1px solid black; border-top: 1px solid black;"> 1. Project Level Statistics </h4>
            {{project_level_content}}
            <h4 style="border-bottom: 1px solid black; border-top: 1px solid black;"> 2. Trends Statistics </h4>
            {{trends_content}}
            <h4 style="border-bottom: 1px solid black; border-top: 1px solid black;"> 3. Regional Level Statistics </h4>
            {{maps_content}}
            <h4 style="border-bottom: 1px solid black; border-top: 1px solid black;"> 4. User Level Statistics </h4>
            {{user_content}}
        </div>
    </div>
    <script type="text/javascript" src="js/materialize.min.js"></script>
</body>
</html>
```

## Notes
- Ensure the HTML template is properly formatted and includes all necessary placeholders.
- The YAML configuration must point to the correct file name of your custom template.
- If additional statistics sections are required, update both the template and data sources accordingly.

By following these steps, you can customize the  File Download Statistics report to fit your specific needs and resource requirements.

