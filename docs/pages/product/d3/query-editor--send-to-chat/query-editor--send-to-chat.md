## Overview

Use the SQL editor in Cube to write, modify, and run queries, and send query results to chat.

## Using the Query Editor

Open the query editor in Cube.

![](e301ec1d-f98e-4df8-9c58-a6ced82293c0-generating-article-cube-query-editor-showing-sql-query-and-results-for-monthly-revenue-analysis.jpeg)

You will see a text box labeled `Edit SQL Query`, where you can write and edit your queries. The `Fields` section to the right lists available fields. Click a field name to add it to the query, or type it directly into the query text box.

You may already see a pre-populated query.

![](bb55ba37-3d96-43cd-8546-a1f2fb02abb0-generating-article-cube-query-editor-interface-showing-sql-query-and-fields-from-orders-view.jpeg)

If so, you can run or modify this query.

### Modifying and Running a Query

To modify the query, edit the text in the `Edit SQL Query` text box. You can add fields from the `Fields` section by clicking on the desired field. You can also type in field names directly.

For example, you could change `DATE_TRUNC('y', orders_view.date)` to `DATE_TRUNC('year', orders_view.date)`.

![](db3d36f2-f91c-44d8-83a0-907cb3e62dec-generating-article-cube-query-editor-interface-showing-sql-query-for-monthly-revenue-calculation-with-fields-panel.jpeg)

After making changes, click the `Run` button to see the updated results.

Click the `Send to chat` button to send the query results to chat. This avoids repetition using CTEs (Common Table Expressions), allows smaller changes to be made easily, and provides feedback to the agent. Consider attaching context to feedback (e.g., "do this instead").

### Using the Results Table

The query results display in a table below the query editor.

![](85317e55-8330-427e-a33b-62f732415cae-generating-article-cube-cloud-query-editor-showing-queried-semantic-model-with-results-table-and-fields-list.jpeg)

A summary section below the table shows summary information for the displayed data.

Use the `Pagination Controls` below the table to navigate through pages of data if there are a lot of results.

![](f94098fb-5b16-465b-80fb-351d97f6fb1e-generating-article-cube-cloud-query-editor-showing-monthly-revenue-data-and-observations.jpeg)

### Closing the Editor

To close the editor, click the close button.

![](5d6ef38c-8831-43b9-99d5-571a991bc8fe-generating-article-cube-query-editor-with-sql-query-and-close-confirmation-dialog.jpeg)

A confirmation modal will appear asking you to confirm.

You can choose to `Send to chat` to save and send the query before closing, or click `Don't save and close` to close without saving.

![](f7a60b73-54e2-47fe-b05e-99e2a29bd50e-generating-article-cube-query-editor-with-sql-code-and-close-confirmation-dialog.jpeg)